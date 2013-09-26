package com.bizosys.hsearch.kv.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.lucene.queryParser.ParseException;
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

public class KVMapperBase {
    	
	private static final byte[] LONG_0 = Storable.putLong(0);
	Set<Integer> neededPositions = null; 
	FieldMapping fm = FieldMapping.getInstance();

	Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	StringBuilder appender = new StringBuilder();
	String tableName = null;
	NV kv = null;

	long joinKeyMaxPoint = -1;
	long joinKeySeekPoint = Long.MAX_VALUE;
	final int joinKeyCacheSize = 100;
	
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

	public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		StringBuilder sb = new StringBuilder();
		
		analyzer = AnalyzerFactory.getInstance().getDefault();
		qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
		
		
		try {
			BufferedReader br = null;
			System.out.println(path);
			
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(new Configuration());
			br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
			String line = null;
			while((line = br.readLine())!=null) {
				sb.append(line);
			}

			fm.parseXMLString(sb.toString());
			neededPositions = fm.sourceSeqWithField.keySet();
			KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
			
			kv = new NV(fm.familyName.getBytes(), "1".getBytes());
			this.tableName = fm.tableName;

		} catch (Exception e) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(e);
		}
	}
	
    protected void map(String[] result, Context context) throws IOException, InterruptedException {
    	
    	//get mergeId 
    	rowId = createMergeId(result);
    	
		//get incremetal value for id
    	if ( joinKeyMaxPoint < joinKeySeekPoint) {
    		joinKeyMaxPoint = createIncrementalValue(rowId, joinKeyCacheSize);
    		if(joinKeyMaxPoint > Integer.MAX_VALUE)
    			throw new IOException("Maximum limit reached please revisit your merge keys in the schema.");
    		joinKeySeekPoint = joinKeyMaxPoint -  joinKeyCacheSize;
    	} else {
    		joinKeySeekPoint++;
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
    		
    		//TODO:call onMap method if needed to modify key and value
    		//KV kv = onMap(new KVIndexer.KV(rowId, fldValue));
    		
    		//if free text index needs to be done
    		if(fld.isDocIndex){
    			mapFreeText(fld, context);
    		}

    		if(!fld.isStored)
    			continue;

    		boolean isEmpty = ( null == rowId) ? true : (rowId.length() == 0);
    		rowKey = ( isEmpty) ? fld.name : rowId + fld.name;
    		appender.delete(0, appender.capacity());
    		rowKey = appender.append(rowKey).append( KVIndexer.FIELD_SEPARATOR)
    						 .append(fld.getDataType()).append(KVIndexer.FIELD_SEPARATOR)
    						 .append( fld.sourceSeq).toString();
    		appender.delete(0, appender.capacity());
    		rowVal = appender.append(joinKeySeekPoint).append(KVIndexer.FIELD_SEPARATOR).append(fldValue).toString();
    		context.write(new Text(rowKey), new Text(rowVal) );
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
        		rowVal = appender.append(joinKeySeekPoint).append(KVIndexer.FIELD_SEPARATOR).append(term.text()).toString();
    			
    			context.write(new Text(rowKey), new Text(rowVal));
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
    }
    
    public long createIncrementalValue(String mergeID, int cacheSize) throws IOException{
    	long id = 0;
    	String row = mergeID + '_' + KVIndexer.INCREMENTAL_ROW;
    	
    	NV kvThisRow = new NV(kv.family, kv.name, LONG_0);
    	RecordScalar scalar = new RecordScalar(row.getBytes(), kvThisRow);
    	if( HReader.exists(tableName, row.getBytes())) {
        	id = HReader.idGenerationByAutoIncr(tableName, scalar, cacheSize);
        	return id;
    	}
    	
    	synchronized (KVMapper.class.getName()) {
        	if( ! HReader.exists(tableName, row.getBytes())) {
        		HWriter.getInstance(false).insertScalar(tableName, scalar);
        	}
		}
    	
    	id = HReader.idGenerationByAutoIncr(tableName, scalar, cacheSize);
    	return id;
    }
}
