package com.bizosys.hsearch.kv.indexing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.util.LineReaderUtil;


public class KVKeyGenerator {

	private static final String EMPTY = "";
	FieldMapping fm = null;
	protected Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	protected Set<Integer> neededPositions = null;
	String partName = null;

	public void initKeyGenerator(Configuration conf) throws IOException{
		StringBuilder sb = new StringBuilder();
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		fm = KVIndexer.createFieldMapping(conf, path, sb);
		KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
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
		/**
		 * This is used for caching. So different schema with same field name may cause a conflict.
		 * Make it unique by adding the tablename at the starting
		 */
		if ( null == partName) {
			partName = fm.tableName.replaceAll("[^A-Za-z0-9]", EMPTY);
			if ( null == partName) partName = "p1";
			else partName = partName + "p1";
		}
		
		rowId = ( rowId.length() > 0 ) ? rowId : partName;
		return rowId;
	}
	
	/**
	 * Simple file as input and output to hbase
	 */
	public static class KVKeyGeneratorMapperFile extends Mapper<LongWritable, Text, Text, Text> {
		
		KVKeyGenerator keyGen = null;
		String result[] = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			keyGen =  new KVKeyGenerator();
			keyGen.initKeyGenerator(context.getConfiguration());
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
	    	if ( null == result) {
	    		ArrayList<String> resultL = new ArrayList<String>();
	    		LineReaderUtil.fastSplit(resultL, value.toString(), KVIndexer.FIELD_SEPARATOR);
	    		result = new String[resultL.size()];
	    	}
	   
	    	Arrays.fill(result, null);

	    	LineReaderUtil.fastSplit(result, value.toString(), KVIndexer.FIELD_SEPARATOR);
	    	
	    	String partitionKey = keyGen.createPartitionKey(result);
			context.write(new Text(partitionKey), value);
		}
	}
	
	public static class KVKeyGeneratorReducerFile extends Reducer<Text, Text, NullWritable, Text>{
		
		StringBuilder sb = null;
		KVKeyGenerator keyGen = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			sb = new StringBuilder();
			keyGen =  new KVKeyGenerator();
			keyGen.initKeyGenerator(context.getConfiguration());
		}
		@Override
		protected void reduce(Text mergeKey, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			String partitionKey = mergeKey.toString();
			int internalId = 0;
			for (Text aRow : values) {
				sb.setLength(0);
				internalId += 1;
				sb.append(aRow.toString()).append(KVIndexer.FIELD_SEPARATOR);
				sb.append(internalId).append(KVIndexer.FIELD_SEPARATOR).append(partitionKey);
				context.write(NullWritable.get(), new Text(sb.toString()));
			}
		}
	}

	
	/**
	 * Hbase as input and output to hbase
	 */
	public static class KVKeyGeneratorMapperHBase extends TableMapper<Text, ImmutableBytesWritable> {
		
		KVKeyGenerator keyGen = null;
		String result[] = null;
		Map<String, Field> sourceNameWithField = null;
		String qualifier = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			keyGen =  new KVKeyGenerator();
			keyGen.initKeyGenerator(context.getConfiguration());
			sourceNameWithField = new HashMap<String, Field>();
			
			int maxSourceSeq = keyGen.fm.nameWithField.size();
			result = new String[maxSourceSeq+1];
			
			for ( Field fld : keyGen.fm.nameWithField.values()) {
				if(!fld.isMergedKey) continue;
				sourceNameWithField.put(fld.sourceName, fld);
			}
		}
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			
			Arrays.fill(result, "");
	    	try {
	    		
	    		Field fld = null;
	    		
				for (KeyValue kv : value.list()) {
					
					byte[] qualifierB = kv.getQualifier();
					int qualifierLen = ( null == qualifierB) ? 0 : qualifierB.length;
					if ( qualifierLen == 0 ) continue;
					
					qualifier = new String(qualifierB);
					byte[] valB = kv.getValue();
					String val = null;
					fld = sourceNameWithField.get(qualifier);
					if ( null == fld) continue;

					int len = ( null == valB) ? 0 : valB.length;
					if ( len > 0 ) {
						val = new String(valB);
					}
					
					result[fld.sourceSeq] = val;
				}
				
				String partitionKey = keyGen.createPartitionKey(result);
				
				context.write(new Text(partitionKey), key);

			} catch (Exception e) {
				throw new IOException("KVKeyGenerator - Error fetching hbase column.");
			}
		}
	}
	
	public static class KVKeyGeneratorReducerHBase extends TableReducer<Text, ImmutableBytesWritable, ImmutableBytesWritable> {

		@Override
		protected void reduce(Text mergeKey, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {

			String partitionKey = mergeKey.toString();
			
			int internalId = 0;
			for (ImmutableBytesWritable rowKey : values) {
				
				//insert internal key in hbase
				Put internalKeyPut = new Put(rowKey.copyBytes());
				internalKeyPut.add(KVIndexer.FAM_NAME, KVIndexer.INTERNAL_KEYB, new Integer(internalId).toString().getBytes());
				internalId++;
				context.write(null, internalKeyPut);
				
				//insert partitionkey in hbase
				Put partitionKeyPut = new Put(rowKey.copyBytes());
				partitionKeyPut.add(KVIndexer.FAM_NAME, KVIndexer.PARTITION_KEYB, partitionKey.getBytes());
				context.write(null, partitionKeyPut);
				
				context.setStatus("Merge key = " + partitionKey + " Internal key = " + internalId);
			}
		}
	}

}
