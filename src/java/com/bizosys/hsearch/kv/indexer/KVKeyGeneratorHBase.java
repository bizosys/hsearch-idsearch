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
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;


public class KVKeyGeneratorHBase {

	private static final String EMPTY = "";
	FieldMapping fm = null;
	protected Map<Integer,String> rowIdMap = new HashMap<Integer, String>();
	protected Set<Integer> neededPositions = null;
	String partName = null;

	public void initKeyGenerator(Configuration conf) throws IOException, ParseException {
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		fm = FieldMapping.getInstance(path);
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
		if ( null == partName) {
				partName = "p";
		}
		
		rowId = ( rowId.length() > 0 ) ? rowId : partName;
		return rowId;
	}
	
	/**
	 * Hbase as input and output to hbase
	 */
	public static class KVKeyGeneratorMapperHBase extends TableMapper<Text, ImmutableBytesWritable> {
		
		KVKeyGeneratorHBase keyGen = null;
		String result[] = null;
		Map<String, Field> sourceNameWithField = null;
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			try {
				keyGen =  new KVKeyGeneratorHBase();
				keyGen.initKeyGenerator(context.getConfiguration());
				sourceNameWithField = new HashMap<String, Field>();
				
				int maxSourceSeq = keyGen.fm.nameWithField.size();
				result = new String[maxSourceSeq+1];
				
				for ( Field fld : keyGen.fm.nameWithField.values()) {
					if(!fld.isMergedKey) continue;
					sourceNameWithField.put(fld.sourceName, fld);
				}
			} catch (ParseException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
		
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context)
				throws IOException, InterruptedException {
			
	    	String family = null;
	    	String qualifier = null;
			Arrays.fill(result, "");
	    	try {
	    		
	    		Field fld = null;
	    		
				for (KeyValue kv : value.list()) {
					
					byte[] qualifierB = kv.getQualifier();
					int qualifierLen = ( null == qualifierB) ? 0 : qualifierB.length;
					if ( qualifierLen == 0 ) continue;
					qualifier = new String(qualifierB);

					byte[] familyB = kv.getFamily();
					int familyLen = ( null == familyB) ? 0 : familyB.length;
					if ( familyLen == 0 ) continue;
					family = new String(familyB);
					
					byte[] valB = kv.getValue();
					String val = null;
					
					String sourceName = family + ":" + qualifier;
					fld = sourceNameWithField.get(sourceName);
					
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
				throw new IOException("KVKeyGenerator - Error fetching hbase column." , e);
			}
		}
	}
	
	public static class KVKeyGeneratorReducerHBase extends TableReducer<Text, ImmutableBytesWritable, ImmutableBytesWritable> {
		
		byte[] familyNameB = null;
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			try {
				
				Configuration conf = context.getConfiguration();
				String path = conf.get(KVIndexer.XML_FILE_PATH);
				FieldMapping fm = FieldMapping.getInstance(path);
				familyNameB = fm.familyName.getBytes();
				
			} catch (ParseException e) {
				e.printStackTrace();
				throw new IOException(e);
			}
		}
		@Override
		protected void reduce(Text mergeKey, Iterable<ImmutableBytesWritable> values, Context context)
				throws IOException, InterruptedException {

			String partitionKey = mergeKey.toString();
			
			int internalId = 0;
			for (ImmutableBytesWritable rowKey : values) {
				
				//insert internal key in hbase
				Put internalKeyPut = new Put(rowKey.copyBytes());
				internalKeyPut.add(familyNameB, KVIndexer.INTERNAL_KEYB, new Integer(internalId).toString().getBytes());
				internalId++;
				context.write(null, internalKeyPut);
				
				//insert partitionkey in hbase
				Put partitionKeyPut = new Put(rowKey.copyBytes());
				partitionKeyPut.add(familyNameB, KVIndexer.PARTITION_KEYB, partitionKey.getBytes());
				context.write(null, partitionKeyPut);
				
				context.setStatus("Merge key = " + partitionKey + " Internal key = " + internalId);
			}
		}
	}

}
