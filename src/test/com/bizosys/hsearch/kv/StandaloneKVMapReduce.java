package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.KVIndexer;
import com.bizosys.hsearch.kv.impl.KVReducer;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.hsearch.util.LuceneUtil;

public class StandaloneKVMapReduce {

	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;

	Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	StringBuilder rowkKeybuilder = new StringBuilder();

	int wordhash;
	String wordhashStr;
	char firstChar;
	char lastChar;
	Analyzer analyzer = null;
	QueryParser qp = null;
	Query q = null;
	KVReducer reducer = new KVReducer();
	String tableName = null;
	
	public StandaloneKVMapReduce(){
		analyzer = new StandardAnalyzer(Version.LUCENE_36);
		qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
	}
	
	public static void main(String[] args) {

	}
	
	public void indexData(final String data, final FieldMapping fm) throws IOException{
		
		this.fm = fm;
		tableName = fm.tableName;
		neededPositions = fm.fieldSeqs.keySet();
		List<String> eachRecord = new ArrayList<String>();
		Map<Text, List<Text>> mappings = new HashMap<Text, List<Text>>();
		LineReaderUtil.fastSplit(eachRecord, data, Initalizer.RECORD_SEPARATOR);
		for (String aLine : eachRecord) {
			if(0 == aLine.length())
				continue;
			String[] result = aLine.split("\\|");
			map(result, mappings);
		}
		
		for (Entry<Text, List<Text>> entry : mappings.entrySet()) {
			reduce(entry.getKey(), entry.getValue());			
		}
		
		System.out.println("Successfully indexed the data");
	}
	
	public void map(String[] result, Map<Text, List<Text>> mappings){
    	int joinKeyPos = -1;
    	String fldValue = "";
    	String rowId = "";
    	String rowKey = "";
    	String rowVal = "";
    	Text key;
    	Text val;
    	rowIdMap.clear();
    	boolean isMergeOrjoinKey = false;

    	for (int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
    		FieldMapping.Field fld = fm.fieldSeqs.get(neededIndex);
    		isMergeOrjoinKey = fld.isMergedKey || fld.isJoinKey;
    		if(!isMergeOrjoinKey)
    			continue;
    		
			fldValue = result[neededIndex];
    		if ( fld.isMergedKey) {
    			rowIdMap.put(fld.mergePosition, fldValue);
    		}if ( fld.isJoinKey ){
    			joinKeyPos = neededIndex;
    		} 
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

		for ( int neededIndex : neededPositions) {
			if(neededIndex < 0)continue;
    		FieldMapping.Field fld = fm.fieldSeqs.get(neededIndex);
    		
    		if ( fld.isMergedKey) continue;
    		if ( fld.isJoinKey ) continue;
    		
    		boolean storedOrIndexed = ( fld.isStored || fld.isIndexable) ; 
    		if ( ! storedOrIndexed ) continue;

    		fldValue = result[neededIndex];
    		boolean isFieldNull =  (fldValue == null) ? true : (fldValue.length() == 0 );
    		if ( fld.skipNull ) {
    			if ( isFieldNull ) continue;
    		} else {
    			if (isFieldNull) fldValue = fld.defaultValue;
    		}
    		//if free text index needs to be done
    		if(fld.isDocIndex){

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
            			rowKey = rowId + firstChar + "_" + lastChar;
            			
                		rowKey = rowkKeybuilder.append(rowKey).append( KVIndexer.FIELD_SEPARATOR ).append( "text" )
                								.append(KVIndexer.FIELD_SEPARATOR).append(fld.name)			   
                								.append(KVIndexer.FIELD_SEPARATOR).append("false").toString();
                		
                		rowkKeybuilder.delete(0, rowkKeybuilder.length());
 
                		rowVal = result[joinKeyPos] + Initalizer.FIELD_SEPARATOR + term.text();
                		
                		key = new Text(rowKey);
                		val = new Text(rowVal);
                		
                		if(mappings.containsKey(key))
                			mappings.get(key).add(val);
                		else{
                			List<Text> values = new ArrayList<Text>();
                			values.add(val);
                			mappings.put(key, values);
                		}
    				}
    				
				} catch (ParseException e) {
					System.err.println("Could not parse for field " + fld.name + " : " + fldValue);
					e.printStackTrace();
				}
    		}
    		
    		if(!fld.isStored)
    			continue;
    		
    		rowkKeybuilder.delete(0, rowkKeybuilder.capacity());
    		isEmpty = ( null == rowId) ? true : (rowId.length() == 0);
    		rowKey = ( isEmpty) ? fld.name : rowId + fld.name;

    		rowKey = rowkKeybuilder.append(rowKey).append(KVIndexer.FIELD_SEPARATOR).append( fld.fieldType )
    											  .append(KVIndexer.FIELD_SEPARATOR).append(fld.name)
					                              .append(KVIndexer.FIELD_SEPARATOR).append(fld.isAnalyzed).toString();
    		
    		rowkKeybuilder.delete(0, rowkKeybuilder.length());
    		
    		rowVal = result[joinKeyPos] + KVIndexer.FIELD_SEPARATOR + fldValue;
    		key = new Text(rowKey);
    		val = new Text(rowVal);
    		
    		if(mappings.containsKey(key))
    			mappings.get(key).add(val);
    		else{
    			List<Text> values = new ArrayList<Text>();
    			values.add(val);
    			mappings.put(key, values);
    		}
		}
	}
	
	public void reduce(Text key, List<Text> values) throws IOException{

		String keyData = key.toString();
    	String[] resultKey = new String[4];
		
    	LineReaderUtil.fastSplit(resultKey, keyData, KVIndexer.FIELD_SEPARATOR);
		
    	String rowKey = resultKey[0];
		String dataType = resultKey[1].toLowerCase();
		String fieldName = resultKey[2];
		boolean isAnalyzed = resultKey[3].equalsIgnoreCase("true") ? true : false;
		
		byte[] finalData = null;
		char dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType);
		
		switch (dataTypeChar) {

			case 't':
				finalData = reducer.indexString(values);
				break;

			case 'e':
				finalData = reducer.indexText(values, isAnalyzed, fieldName);
				break;

			case 'i':
				finalData = reducer.indexInteger(values);
				break;

			case 'f':
				finalData = reducer.indexFloat(values);
				break;

			case 'd':
				finalData = reducer.indexDouble(values);
				break;

			case 'l':
				finalData = reducer.indexLong(values);
				break;
			
			case 'b':
				finalData = reducer.indexBoolean(values);
				break;

			case 'c':
				finalData = reducer.indexByte(values);
				break;

			default:
				break;
		}
		
		if(null == finalData)return;
		
		NV kv = new NV(KVIndexer.FAM_NAME,KVIndexer.COL_NAME, finalData);
		RecordScalar record = new RecordScalar(rowKey.getBytes(), kv);
		HWriter.getInstance(false).insertScalar(tableName, record);
	}
}
