package com.bizosys.hsearch.kv.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.kv.KVIndexer.KV;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.hsearch.util.LuceneUtil;

public class KVMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
	String[] result = null;
	Set<Integer> neededPositions = null; 
	FieldMapping fm = FieldMapping.getInstance();

	Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	StringBuilder rowkKeybuilder = new StringBuilder();

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

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		StringBuilder sb = new StringBuilder();
		//TODO:Needs to be taken from xml file
		analyzer = new StandardAnalyzer(Version.LUCENE_36);
		qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
		
		try {
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
			String line = null;
			while((line = br.readLine())!=null) {
				sb.append(line);
			}

			fm.parseXMLString(sb.toString());
			neededPositions = fm.fieldSeqs.keySet();
			KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;

		} catch (Exception e) {
			System.err.println("Cannot read from path " + path);
		}
	}
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	if ( null == result) {
    		result = value.toString().split("|");
    	}
    	Arrays.fill(result, null);
    	LineReaderUtil.fastSplit(result, value.toString(), KVIndexer.FIELD_SEPARATOR);
    	int joinKeyPos = -1;
    	String fldValue = "";
    	String rowId = "";
    	String rowKey = "";
    	String rowVal = "";
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
    		
    		//TODO:call onMap method if needed to modify key and value
    		//KV kv = onMap(new KVIndexer.KV(rowId, fldValue));
    		
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
                								.append(KVIndexer.FIELD_SEPARATOR).append("false")
                								.append(KVIndexer.FIELD_SEPARATOR).append("false").toString();
                		
                		rowkKeybuilder.delete(0, rowkKeybuilder.length());
 
                		rowVal = result[joinKeyPos] + KVIndexer.FIELD_SEPARATOR + term.text();
            			
            			context.write(new Text(rowKey), new Text(rowVal));
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
					                              .append(KVIndexer.FIELD_SEPARATOR).append(fld.isAnalyzed)
					                              .append(KVIndexer.FIELD_SEPARATOR).append(fld.isRepeatable)
					                              .toString();
    		
    		rowkKeybuilder.delete(0, rowkKeybuilder.length());
    		
    		rowVal = result[joinKeyPos] + KVIndexer.FIELD_SEPARATOR + fldValue;
    		
      		context.write(new Text(rowKey), new Text(rowVal));
    	}
    }
}
