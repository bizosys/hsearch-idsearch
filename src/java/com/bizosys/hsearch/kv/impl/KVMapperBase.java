package com.bizosys.hsearch.kv.impl;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
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
import com.bizosys.hsearch.util.LuceneUtil;
import com.bizosys.unstructured.AnalyzerFactory;
import com.bizosys.unstructured.util.IdSearchLog;

public class KVMapperBase {
    	
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
	long mergeKeySeekPosition = 0;
	Map<String, Counter> ids = new HashMap<String, Counter>();
	
	String fldValue = null;
	boolean isFieldNull = false;
	String rowKey = "";
	String rowVal = "";
	String rowId = "";

	int wordhash;
	String wordhashStr;
	char firstChar;
	char lastChar;
	QueryParser qp = null;
	Query q = null;
	Analyzer analyzer = null;

	public KV onMap(KV kv ) {
		return kv;
	}

	@SuppressWarnings("rawtypes")
	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		StringBuilder sb = new StringBuilder();
		
		analyzer = AnalyzerFactory.getInstance().getDefault();
		if ( IdSearchLog.l.isDebugEnabled()) {
			IdSearchLog.l.debug("Using Analyzer :" + analyzer);
		}
		qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
		
		
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
		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(fex);
		} catch (ParseException pex) {
			System.err.println("Cannot Parse File " + path);
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
    	rowId = createMergeId(result);
    	
		//get incremetal value for id
		if(ids.containsKey(rowId)){
			Counter c = ids.get(rowId);
			if(c.maxPointer <= c.seekPointer){
				c.maxPointer = createIncrementalValue(rowId, mergeKeyCacheSize);
        		if(c.maxPointer > Integer.MAX_VALUE)
        			throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
				c.seekPointer = c.maxPointer - mergeKeyCacheSize;
				mergeKeySeekPosition = c.seekPointer++;
			} else{
				mergeKeySeekPosition = c.seekPointer++;
			}
		} else {
			long maxPointer = createIncrementalValue(rowId, mergeKeyCacheSize);
    		if(maxPointer > Integer.MAX_VALUE)
    			throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
			long seekPointer = maxPointer - mergeKeyCacheSize;
			Counter c = new Counter(seekPointer, maxPointer);
			mergeKeySeekPosition = c.seekPointer++;
			ids.put(rowId, c);
		}

		for ( int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
    		FieldMapping.Field fld = fm.sourceSeqWithField.get(neededIndex);
    		
    		if ( fld.isMergedKey ) continue;
    		
    		fldValue = result[neededIndex];
    		
    		isFieldNull =  (fldValue == null) ? true : (fldValue.length() == 0 );
    		if ( fld.skipNull ) {
    			if ( isFieldNull ) continue;
    		} else {
    			if (isFieldNull) fldValue = fld.defaultValue;
    		}
    		
    		if(fld.isDocIndex) {
    			mapFreeText(fld, context);
    		}
    		
    		if(fld.isStored) {
        		boolean isEmpty = ( null == rowId) ? true : (rowId.length() == 0);
        		rowKey = ( isEmpty) ? fld.name : rowId + fld.name;
        		appender.delete(0, appender.capacity());
        		rowKey = appender.append(rowKey).append( KVIndexer.FIELD_SEPARATOR)
        						 .append(fld.getDataType()).append(KVIndexer.FIELD_SEPARATOR)
        						 .append( fld.sourceSeq).toString();
        		appender.delete(0, appender.capacity());
        		rowVal = appender.append(mergeKeySeekPosition).append(KVIndexer.FIELD_SEPARATOR).append(fldValue).toString();
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
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public void mapFreeText(Field fld, Context context) throws IOException, InterruptedException{
    	try {
			Set<Term> terms = new HashSet<Term>();
			if( !isFieldNull){
				fldValue = LuceneUtil.escapeLuceneSpecialCharacters(fldValue);
				q = qp.parse(fldValue);
				q.extractTerms(terms);				
			}
			
			for (Term term : terms) {
				wordhash = Hashing.hash(term.text());
    			wordhashStr = new Integer(wordhash).toString();
    			firstChar = wordhashStr.charAt(0);
    			lastChar = wordhashStr.charAt(wordhashStr.length() - 1);
        		
    			appender.delete(0, appender.capacity());
    			//below is hardcoded datatype for free text search.
    			rowKey = appender.append(rowId).append(firstChar).append('_').append(lastChar)
    							  .append(KVIndexer.FIELD_SEPARATOR).append("text")
								 .append( KVIndexer.FIELD_SEPARATOR ).append(fld.sourceSeq).toString();

    			appender.delete(0, appender.capacity());
        		rowVal = appender.append(mergeKeySeekPosition).append(KVIndexer.FIELD_SEPARATOR).append(wordhash).toString();
    			
    			context.write(new Text(rowKey), new Text(rowVal));
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
