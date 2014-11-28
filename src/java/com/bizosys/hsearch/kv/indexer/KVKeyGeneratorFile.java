/**
*    Copyright 2014, Bizosys Technologies Pvt Ltd
*
*    This software and all information contained herein is the property
*    of Bizosys Technologies.  Much of this information including ideas,
*    concepts, formulas, processes, data, know-how, techniques, and
*    the like, found herein is considered proprietary to Bizosys
*    Technologies, and may be covered by U.S., India and foreign patents or
*    patents pending, or protected under trade secret laws.
*    Any dissemination, disclosure, use, or reproduction of this
*    material for any reason inconsistent with the express purpose for
*    which it has been disclosed is strictly forbidden.
*
*                        Restricted Rights Legend
*                        ------------------------
*
*    Use, duplication, or disclosure by the Government is subject to
*    restrictions as set forth in paragraph (b)(3)(B) of the Rights in
*    Technical Data and Computer Software clause in DAR 7-104.9(a).
*/

package com.bizosys.hsearch.kv.indexer;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.util.LineReaderUtil;


public class KVKeyGeneratorFile {

	private static final String EMPTY = "";
	FieldMapping fm = null;
	protected Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	protected Set<Integer> neededPositions = null;
	String partName = null;
	boolean skipHeader = false;
	char separator = '\t';
	
	public void initKeyGenerator(Configuration conf) throws IOException, ParseException {
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		skipHeader = conf.getBoolean(KVIndexer.SKIP_HEADER, false);
		fm = FieldMapping.getInstance(path);
		separator = fm.fieldSeparator;
		neededPositions = fm.sourceSeqWithField.keySet();
	}

	public String createPartitionKey(String[] result){

		rowIdMap.clear();
		String fldValue = null;
		String rowId = EMPTY;

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

		boolean isFirst = true;
		for (int j = 0; j < megedKeyArr.length; j++) {
	
			if(isFirst)
				isFirst = false;
			else
				rowId = rowId + "_";
	
			rowId = rowId + megedKeyArr[j]; 
		}
		if ( null == partName) {
			partName = "p";
		}
		
		rowId = ( rowId.length() > 0 ) ? rowId : partName;
		return rowId;
	}
	
	/**
	 * Simple file as input and output to hbase
	 */
	public static class KVKeyGeneratorMapperFile extends Mapper<LongWritable, Text, Text, Text> {
		
		KVKeyGeneratorFile keyGen = null;
		String result[] = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			try {
				keyGen =  new KVKeyGeneratorFile();
				keyGen.initKeyGenerator(context.getConfiguration());
			} catch (ParseException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			if(keyGen.skipHeader){
				keyGen.skipHeader = false;
	    		if ( 0 == key.get()) return;
	    	}
			
	    	if ( null == result) {
	    		ArrayList<String> resultL = new ArrayList<String>();
	    		LineReaderUtil.fastSplit(resultL, value.toString(), keyGen.separator);
	    		result = new String[resultL.size()];
	    	}
	   
	    	Arrays.fill(result, null);

	    	LineReaderUtil.fastSplit(result, value.toString(), keyGen.separator);
	    	
	    	String partitionKey = keyGen.createPartitionKey(result);
			context.write(new Text(partitionKey), value);
		}
	}
	
	public static class KVKeyGeneratorReducerFile extends Reducer<Text, Text, NullWritable, Text>{
		
		StringBuilder sb = null;
		KVKeyGeneratorFile keyGen = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			try {
				
				sb = new StringBuilder();
				keyGen =  new KVKeyGeneratorFile();
				keyGen.initKeyGenerator(context.getConfiguration());
				
			} catch (ParseException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
		@Override
		protected void reduce(Text mergeKey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String partitionKey = mergeKey.toString();
			int internalId = 0;
			for (Text aRow : values) {
				sb.setLength(0);
				sb.append(aRow.toString()).append(keyGen.separator);
				sb.append(internalId).append(keyGen.separator).append(partitionKey);
				context.write(NullWritable.get(), new Text(sb.toString()));
				internalId++;
			}
		}
	}
}
