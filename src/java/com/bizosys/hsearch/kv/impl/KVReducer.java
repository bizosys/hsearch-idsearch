package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.kv.KVIndexer.KV;
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

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	String keyData = key.toString();
    	String[] resultKey = new String[5];
		
    	LineReaderUtil.fastSplit(resultKey, keyData, KVIndexer.FIELD_SEPARATOR);
		
    	String rowKey = resultKey[0];
		String dataType = resultKey[1].toLowerCase();
		String fieldName = resultKey[2];
		boolean isAnalyzed = resultKey[3].equalsIgnoreCase("true") ? true : false;
		boolean isRepetable = resultKey[4].equalsIgnoreCase("true") ? true : false;
		
		
		char dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType);
		
		//TODO:call onReduce method to modify key and value if needed

		byte[] finalData = cookBytes(values, fieldName, isAnalyzed, isRepetable, dataTypeChar);
		
		if(null == finalData)return;
		Put put = new Put(rowKey.getBytes());
        put.add(KVIndexer.FAM_NAME,KVIndexer.COL_NAME, finalData);
        
        context.write(null, put);
	}

	public byte[] cookBytes(Iterable<Text> values, String fieldName,
			boolean isAnalyzed, boolean isRepetable, char dataTypeChar) throws IOException {
		
		byte[] finalData = null;
		switch (dataTypeChar) {

			case 't':
				finalData = IndexFieldString.cook(values, isRepetable);
				break;

			case 'e':
				finalData = indexText(values, isAnalyzed, fieldName);
				break;

			case 'i':
				finalData = IndexFieldInteger.cook(values, isRepetable);
				break;

			case 'f':
				finalData = IndexFieldFloat.cook(values, isRepetable);
				break;

			case 'd':
				finalData = IndexFieldDouble.cook(values, isRepetable);
				break;

			case 'l':
				finalData = IndexFieldLong.cook(values, isRepetable);
				break;
			
			case 'b':
				finalData = IndexFieldBoolean.cook(values, isRepetable);
				break;

			case 'c':
				finalData = IndexFieldByte.cook(values, isRepetable);
				break;

			default:
				break;
		}
		return finalData;
	}
	
	public byte[] indexText(Iterable<Text> values, boolean isAnalyzed, String fieldName) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
		int containerKey = 0;
		String containervalue = null;
    	String[] resultValue = new String[2];

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_36);
    	Map<String, Integer> docTypes = new HashMap<String, Integer>(1);
		Map<String, Integer> fldTypes = new HashMap<String, Integer>(1);

		String docTypeName = "*";
		String fieldTypeName = fieldName;
		docTypes.put(docTypeName, 1);
		fldTypes.put(fieldTypeName, 1);

		KVDocIndexer indexer = new KVDocIndexer();
		String line = null;
		
		Map<Integer, String> docIdWithFieldValue = new HashMap<Integer, String>();
		
		for (Text text : values) {
			
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();

			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			
			containerKey = (null == resultValue[0]) ? Integer.MAX_VALUE : Integer.parseInt(resultValue[0]);
			containervalue = (null == resultValue[1]) ? "" : resultValue[1];
			hasValue = true;
			docIdWithFieldValue.put(containerKey, containervalue);
		}
		
		if(hasValue){
			try {
				indexer.addDoumentTypes(docTypes);
				indexer.addFieldTypes(fldTypes);
				indexer.addToIndex(analyzer, docTypeName, fieldTypeName, docIdWithFieldValue, isAnalyzed);
				finalData = indexer.toBytes();
				indexer.close();
				
			} catch (InstantiationException e) {
				e.printStackTrace(System.err);
				System.err.println("InstantiationException for docType and fieldType " + e.getMessage());
			}
		}
		return finalData;
	}
	
}
