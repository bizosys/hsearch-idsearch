package com.bizosys.hsearch.kv.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.kv.KVIndexer.KV;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.hsearch.util.LuceneUtil;
import com.bizosys.unstructured.AnalyzerFactory;

public class KVMapperBase {
    	
	private static final char PLUS = '+';
	private static final byte[] LONG_0 = Storable.putLong(0);
	private static class Counter{
		public long seekPointer = -1;
		public long maxPointer = Long.MAX_VALUE;
		
		public Counter(long seekPointer, long maxPointer){
			this.seekPointer = seekPointer;
			this.maxPointer = maxPointer;
		}
	}

	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;

	Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	StringBuilder appender = new StringBuilder();
	String tableName = null;
	NV kv = null;

	final int mergeKeyCacheSize = 1000;
	long incrementalIdSeekPosition = 0;
	Map<String, Counter> ids = new HashMap<String, Counter>();
	
	String fldValue = null;
	boolean isFieldNull = false;
	String rowKey = "";
	String rowVal = "";
	String mergeId = "";

	Map<String, QueryParser> fieldNameWithQueryParser = new HashMap<String, QueryParser>();

	public KV onMap(KV kv ) {
		return kv;
	}

	@SuppressWarnings("rawtypes")
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		StringBuilder sb = new StringBuilder();
		
		try {

			BufferedReader br = null;
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			if ( fs.exists(hadoopPath) ) {
				br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while((line = br.readLine())!=null) {
					sb.append(line);
				}
				fm = new FieldMapping();
				fm.parseXMLString(sb.toString());
			} else {
				fm = FieldMapping.getInstance(path);
			}

			AnalyzerFactory.getInstance().init(fm);

		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(fex);
		} catch (ParseException pex) {
			System.err.println("Cannot Parse File " + path);
			throw new IOException(pex);
		} catch (Exception pex) {
			System.err.println("Error : " + path);
			throw new IOException(pex);
		}
		
		neededPositions = fm.sourceSeqWithField.keySet();
		KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
		
		kv = new NV(fm.familyName.getBytes(), "1".getBytes());
		this.tableName = fm.tableName;

	}
	
    @SuppressWarnings({ "rawtypes", "unchecked" })
	protected void map(String[] result, Context context) throws IOException, InterruptedException {

    	//get mergeId 
    	mergeId = createMergeId(result);
    	
		//get incremetal value for id
		if(ids.containsKey(mergeId)){
			Counter c = ids.get(mergeId);
			if(c.maxPointer <= c.seekPointer){
				c.maxPointer = createIncrementalValue(mergeId, mergeKeyCacheSize);
        		if(c.maxPointer > Integer.MAX_VALUE)
        			throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
				c.seekPointer = c.maxPointer - mergeKeyCacheSize;
				incrementalIdSeekPosition = c.seekPointer++;
			} else{
				incrementalIdSeekPosition = c.seekPointer++;
			}
		} else {
			long maxPointer = createIncrementalValue(mergeId, mergeKeyCacheSize);
    		if(maxPointer > Integer.MAX_VALUE)
    			throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
			long seekPointer = maxPointer - mergeKeyCacheSize;
			Counter c = new Counter(seekPointer, maxPointer);
			incrementalIdSeekPosition = c.seekPointer++;
			ids.put(mergeId, c);
		}

		for ( int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
    		FieldMapping.Field fld = fm.sourceSeqWithField.get(neededIndex);
    		
    		if ( fld.isMergedKey ) continue;
    		
    		if ( fld.isDocIndex ) {
    			boolean hasExpression = ( null == fld.expression) ? false : (fld.expression.trim().length() > 0 );
    			if ( hasExpression ) {
    	    		fldValue = evalExpression(fld.expression, result);
    			} else {
        			fldValue =  result[neededIndex];
    			}
    		} else {
    			fldValue =  result[neededIndex];
    		}
    		
    		isFieldNull =  (fldValue == null) ? true : (fldValue.length() == 0 );
    		if ( fld.skipNull ) {
    			if ( isFieldNull ) continue;
    		} else {
    			if (isFieldNull) fldValue = fld.defaultValue;
    		}
    		
    		if(fld.isDocIndex) {
    			if ( fld.isRepeatable) mapFreeTextBitset(fld, context);
    			else mapFreeTextSet(fld, context);
    		}
    		
    		if(fld.isStored) {
        		boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
        		rowKey = ( isEmpty) ? fld.name : mergeId + fld.name;
        		appender.delete(0, appender.capacity());
        		rowKey = appender.append(rowKey).append( KVIndexer.FIELD_SEPARATOR)
        						 .append(fld.getDataType()).append(KVIndexer.FIELD_SEPARATOR)
        						 .append( fld.sourceSeq).toString();
        		appender.delete(0, appender.capacity());
        		rowVal = appender.append(incrementalIdSeekPosition).append(KVIndexer.FIELD_SEPARATOR).append(fldValue).toString();
        		context.write(new Text(rowKey), new Text(rowVal) );
    		}

    	}
    }
    
    public String createMergeId(String[] result){

    	rowIdMap.clear();
    	String fldValue = null;
    	String rowId = "";
    	
    	for (int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
    		FieldMapping.Field fld = fm.sourceSeqWithField.get(neededIndex);
    		if(!fld.isMergedKey) continue;
			fldValue = result[neededIndex];
			if ( null == fldValue) fldValue = fld.defaultValue;
			else if (fldValue.trim().length() == 0  ) fldValue = fld.defaultValue;
			rowIdMap.put(fld.mergePosition, fldValue);
    	}
		
		String[] megedKeyArr = new String[rowIdMap.size()];

		for (Integer mergePosition : rowIdMap.keySet()) {
			megedKeyArr[mergePosition] = rowIdMap.get(mergePosition);
		}
		
		boolean isEmpty = false;
		for (int j = 0; j < megedKeyArr.length; j++) {
			isEmpty = ( null == megedKeyArr[j]) ? true : (megedKeyArr[j].length() == 0);
			if(isEmpty)rowId += "-1" + "_";
			else rowId += megedKeyArr[j] + "_";
		}
		return rowId;
    }

	public final String evalExpression(final String sourceNames, final String[] result){
		List<String> names = new ArrayList<String>();
		LineReaderUtil.fastSplit(names, sourceNames, PLUS);
		appender.delete(0, appender.capacity());
		String tempValue = null;
		boolean isEmpty = false;
		for (String name : names) {
			Field fld = fm.nameWithField.get(name);
			tempValue = result[fld.sourceSeq];
			isEmpty = (null == tempValue) ? true : tempValue.length() == 0;
			if(isEmpty)
				continue;
			appender.append(tempValue).append(" ");
		}
		
		return appender.toString();
	}

    @SuppressWarnings({ "rawtypes", "unchecked" })
	public void mapFreeTextSet(Field fld, Context context) throws IOException, InterruptedException{
		
    	QueryParser qp = getQueryParser(fld);
    	
    	int wordhash;
    	String wordhashStr;
    	char firstChar;
    	char lastChar;    	

		try {
			Set<Term> terms = new HashSet<Term>();
			if( !isFieldNull){
				fldValue = LuceneUtil.escapeLuceneSpecialCharacters(fldValue);
				Query q = qp.parse(fldValue);
				q.extractTerms(terms);				
			}
			
			for (Term term : terms) {
				wordhash = Hashing.hash(term.text());
    			wordhashStr = new Integer(wordhash).toString();
    			firstChar = wordhashStr.charAt(0);
    			lastChar = wordhashStr.charAt(wordhashStr.length() - 1);
        		
    			appender.delete(0, appender.capacity());
    			//below is hardcoded datatype for free text search.
    			rowKey = appender.append(mergeId).append(firstChar).append('_').append(lastChar)
    							  .append(KVIndexer.FIELD_SEPARATOR).append("text")
								 .append( KVIndexer.FIELD_SEPARATOR ).append(fld.sourceSeq).toString();

    			appender.delete(0, appender.capacity());
        		rowVal = appender.append(incrementalIdSeekPosition).append(KVIndexer.FIELD_SEPARATOR).append(wordhash).toString();
    			
    			context.write(new Text(rowKey), new Text(rowVal));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }

	private QueryParser getQueryParser(Field fld) {
		QueryParser qp = null;
    	Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fld.name);
		if ( fieldNameWithQueryParser.containsKey(fld.name)) { 
			qp = fieldNameWithQueryParser.get(fld.name);
		} else {
	    	qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
	    	fieldNameWithQueryParser.put(fld.name, qp);
		}
		return qp;
	}
    
	Set<Term> terms = new LinkedHashSet<Term>();

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void mapFreeTextBitset(Field fld, Context context) throws IOException, InterruptedException{

		terms.clear();
		
		try {
			if( !isFieldNull){
				fldValue = LuceneUtil.escapeLuceneSpecialCharacters(fldValue);
		    	QueryParser qp = getQueryParser(fld);
		    	Query q = qp.parse(fldValue);
				q.extractTerms(terms);				
			}
			
			String last2 = null;
			String last1 = null;
			
			for (Term term : terms) {
    			String termWord = term.text();
    			appender.delete(0, appender.capacity());
    			
    			/**
    			 * Row Key is mergeidFIELDwordhashStr
    			 */
        		boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
        		String rowKeyPrefix = ( isEmpty) ? fld.name : mergeId + fld.name;
    			
    			rowKey = appender.append(rowKeyPrefix).append(termWord).append(KVIndexer.FIELD_SEPARATOR)
    					.append("text").append( KVIndexer.FIELD_SEPARATOR )
    					.append(fld.sourceSeq).toString();

    			appender.setLength(0);
        		rowVal = appender.append(incrementalIdSeekPosition).toString();
    			
    			context.write(new Text(rowKey), new Text(rowVal));
    			
    			if ( ! fld.biWord && !fld.triWord) continue;

    			/**
    			 * Do Three phrase word
    			 */
    			if ( null != last2) {
    				appender.setLength(0);
    				
        			rowKey = appender.append(rowKeyPrefix).append(last2).append(' ').
        					append(last1).append(' ').append(termWord).append('*').
        					append(KVIndexer.FIELD_SEPARATOR).
        					append("text").append( KVIndexer.FIELD_SEPARATOR ).
        					append(fld.sourceSeq).toString();
        			context.write(new Text(rowKey), new Text(rowVal));
    			}
    			
    			/**
    			 * Do Two phrase word
    			 */
    			if ( null != last1) {
    				appender.setLength(0);
    				
        			rowKey = appender.append(rowKeyPrefix).
        					append(last1).append(' ').append(termWord).append('*').
        					append(KVIndexer.FIELD_SEPARATOR).
        					append("text").append( KVIndexer.FIELD_SEPARATOR ).
        					append(fld.sourceSeq).toString();
        			context.write(new Text(rowKey), new Text(rowVal));
    			}
    			
    			last2 = last1;
    			last1 = termWord;
    			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
    }    
    
    public long createIncrementalValue(String mergeID, int cacheSize) throws IOException{
    	long id = 0;
    	String row = mergeID + KVIndexer.INCREMENTAL_ROW;
    	
    	NV kvThisRow = new NV(kv.family, kv.name, LONG_0);
    	RecordScalar scalar = new RecordScalar(row.getBytes(), kvThisRow);
    	if(! HReader.exists(tableName, row.getBytes())){
        	synchronized (KVMapperBase.class.getName()) {
            	if( ! HReader.exists(tableName, row.getBytes())) {
            		HWriter.getInstance(false).insertScalar(tableName, scalar);
            	}
			}        		
    	}
		id = HReader.idGenerationByAutoIncr(tableName, scalar, cacheSize);
    	return id;
    }
}
