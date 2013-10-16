package com.bizosys.hsearch.kv.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.iq80.snappy.Snappy;

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.kv.KVIndexer.KV;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVIndex;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldBoolean;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldByte;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldDouble;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldFloat;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldInteger;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldLong;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldString;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	public KV onReduce(KV kv ) {
		return kv;
	}
	
	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		try {
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
			String line = null;
			StringBuilder sb = new StringBuilder();
			while((line = br.readLine())!=null) {
				sb.append(line);
			}

			fm = new FieldMapping();
			fm.parseXMLString(sb.toString());
			neededPositions = fm.sourceSeqWithField.keySet();
			KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
			conf.set(KVIndexer.TABLE_NAME, fm.tableName);
			
		} catch (Exception e) {
			System.err.println("Cannot read from path " + path);
			throw new IOException("Cannot read from path " + path);
		}
	}	

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String keyData = key.toString();
    	String[] resultKey = new String[3];
		
    	LineReaderUtil.fastSplit(resultKey, keyData, KVIndexer.FIELD_SEPARATOR);
		
    	String rowKey = resultKey[0];
    	String dataType = resultKey[1].toLowerCase();
    	int sourceSeq = Integer.parseInt(resultKey[2]);
    	
    	Field fld = fm.sourceSeqWithField.get(sourceSeq);
		char dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType);
		
		byte[] finalData = cookBytes(rowKey, values, fld, dataTypeChar);
		if(null == finalData) return;

		Put put = new Put(rowKey.getBytes());
        put.add(KVIndexer.FAM_NAME,KVIndexer.COL_NAME, finalData);
        
        context.write(null, put);
	}

	private byte[] cookBytes(String key, Iterable<Text> values, 
			Field fld, char dataTypeChar) throws IOException {
		
		byte[] finalData = null;
		String fieldName = fld.name;
		boolean compressed = fld.isCompressed;
		boolean repeatable = fld.isRepeatable;
		boolean analyzed = fld.isAnalyzed;
		
		switch (dataTypeChar) {

			case 't':
				finalData = IndexFieldString.cook(values, repeatable, compressed);
				break;

			case 'e':
				
				/**
				 * Skip multi phrases which are only sighted once.
				 */
				boolean skipSingle = ( key.charAt(key.length() - 1) == '*');
				
				finalData = ( repeatable ) ? 
					indexTextBitset(skipSingle, values, analyzed, fieldName, compressed) : 
					indexTextSet(skipSingle, values, analyzed, fieldName);
				break;

			case 'i':
				finalData = IndexFieldInteger.cook(values, repeatable, compressed);
				break;

			case 'f':
				finalData = IndexFieldFloat.cook(values, repeatable, compressed);
				break;

			case 'd':
				finalData = IndexFieldDouble.cook(values, repeatable, compressed);
				break;

			case 'l':
				finalData = IndexFieldLong.cook(values, repeatable, compressed);
				break;
			
			case 'b':
				finalData = IndexFieldBoolean.cook(values, repeatable, compressed);
				break;

			case 'c':
				finalData = IndexFieldByte.cook(values, repeatable, compressed);
				break;

			default:
				break;
		}
		return finalData;
	}
	
	Set<Integer> records = new HashSet<Integer>(3);
	public byte[] indexTextSet(boolean skipSingle, Iterable<Text> values, boolean isAnalyzed, String fieldName) throws IOException {
		
		byte[] finalData = null;
		int containerKey = 0;
		int containervalue = 0;
		String[] resultValue = new String[2];
		HSearchTableKVIndex table = new HSearchTableKVIndex();
		String line = null;
		
		int docType = 1;
		int fieldType = 1;
		String metaDoc = "-";
		boolean flag = true;
		
		records.clear();
		for (Text text : values) {

			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();

			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);

			containerKey = Integer.parseInt(resultValue[0]);
			if(null == resultValue[1]) continue;
			containervalue = Integer.parseInt(resultValue[1]);
			table.put(docType, fieldType, metaDoc, containervalue, containerKey, flag);
			
			if ( skipSingle) {
				if ( records.size() < 2 ) records.add(containerKey);
			}
			
		}
		
		if ( skipSingle && records.size() < 2) return null;

		finalData = table.toBytes();		
		return finalData;
	}
	
	public byte[] indexTextBitset(boolean skipSingle, Iterable<Text> values, boolean isAnalyzed, String fieldName, boolean isCompressed) throws IOException {
		
		byte[] finalData = null;
		int containerKey = 0;
		BitSet foundIds = new BitSet();
		
		for (Text text : values) {
			if ( null == text) continue;
			containerKey = Integer.parseInt(text.toString());
			foundIds.set(containerKey);
		}
		
		if ( skipSingle && foundIds.cardinality() < 2) return null;

		finalData = SortedBytesBitset.getInstanceBitset().bitSetToBytes(foundIds);
		if ( null == finalData) return finalData;
		
		if ( isCompressed ) {
			return Snappy.compress(finalData);
		} 
		return finalData;
	}	
	
}
