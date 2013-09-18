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

import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.kv.impl.KVIndexer.KV;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {

	public KV onReduce(KV kv ) {
		return kv;
	}

	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	String keyData = key.toString();
    	String[] resultKey = new String[4];
		
    	LineReaderUtil.fastSplit(resultKey, keyData, KVIndexer.FIELD_SEPARATOR);
		
    	String rowKey = resultKey[0];
		String dataType = resultKey[1].toLowerCase();
		String fieldName = resultKey[2];
		boolean isAnalyzed = resultKey[3].equalsIgnoreCase("true") ? true : false;
		
		byte[] finalData = null;
		char dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType);
		
		//TODO:call onReduce method to modify key and value if needed

		switch (dataTypeChar) {

			case 't':
				finalData = indexString(values);
				break;

			case 'e':
				finalData = indexText(values, isAnalyzed, fieldName);
				break;

			case 'i':
				finalData = indexInteger(values);
				break;

			case 'f':
				finalData = indexFloat(values);
				break;

			case 'd':
				finalData = indexDouble(values);
				break;

			case 'l':
				finalData = indexLong(values);
				break;
			
			case 'b':
				finalData = indexBoolean(values);
				break;

			case 'c':
				finalData = indexByte(values);
				break;

			default:
				break;
		}
		
		if(null == finalData)return;
		Put put = new Put(rowKey.getBytes());
        put.add(KVIndexer.FAM_NAME,KVIndexer.COL_NAME, finalData);
        
        context.write(null, put);
	}

	public byte[] indexByte(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;
		Cell2<Integer,Byte> nonrepeatableCell = new Cell2<Integer, Byte>
				(SortedBytesInteger.getInstance(),SortedBytesChar.getInstance());
		
		for (Text text : values) {
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			int containerKey = Integer.parseInt(resultValue[0]);
			byte containervalue = resultValue[1].getBytes()[0];	
			
			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.ByteComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
		}
		return finalData;
	}

	public byte[] indexBoolean(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;

		Cell2<Integer,Boolean> nonrepeatableCell = new Cell2<Integer, Boolean>
				(SortedBytesInteger.getInstance(),SortedBytesBoolean.getInstance());
		
		for (Text text : values) {
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			int containerKey = Integer.parseInt(resultValue[0]);
			boolean containervalue = resultValue[1].equalsIgnoreCase("true") ? true : false;	
			
			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.BooleanComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
		}
		return finalData;
	}

	public byte[] indexLong(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;

		Cell2<Integer,Long> nonrepeatableCell = new Cell2<Integer, Long>
				(SortedBytesInteger.getInstance(),SortedBytesLong.getInstance());
		
		for (Text text : values) {
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			int containerKey = Integer.parseInt(resultValue[0]);
			long containervalue = Long.parseLong(resultValue[1]);	
			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.LongComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
		}
		return finalData;
	}

	public byte[] indexDouble(Iterable<Text> values)
			throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;

		Cell2<Integer,Double> nonrepeatableCell = new Cell2<Integer, Double>
			(SortedBytesInteger.getInstance(),SortedBytesDouble.getInstance());
		
		for (Text text : values) {
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			int containerKey = Integer.parseInt(resultValue[0]);
			double containervalue = Double.parseDouble(resultValue[1]);
			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.DoubleComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
		}
		
		return finalData;
	}

	public byte[] indexFloat(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;

		Cell2<Integer,Float> nonrepeatableCell = new Cell2<Integer, Float>
			(SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
		
		for (Text text : values) {
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			int containerKey = Integer.parseInt(resultValue[0]);
			float containervalue = 0.0f;
			containervalue = Float.parseFloat(resultValue[1]);
			
			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.FloatComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
		}
		return finalData;
	}

	public byte[] indexInteger(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;

		Cell2<Integer,Integer> nonrepeatableCell = new Cell2<Integer, Integer>
				(SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());
		
		for (Text text : values) {
			
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			int containerKey = Integer.parseInt(resultValue[0]);
			int containervalue = 0;
			containervalue = Integer.parseInt(resultValue[1]);

			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.IntegerComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
		}
		return finalData;
	}

	public byte[] indexString(Iterable<Text> values) throws IOException {
		byte[] finalData = null;
		boolean hasValue = false;
		int containerKey = 0;
		String containervalue = null;
		
    	String[] resultValue = new String[2];
    	String line = null;

		Cell2<Integer,String> nonrepeatableCell = new Cell2<Integer, String>
			(SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
		for (Text text : values) {
			
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();

			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);
			
			containerKey = Integer.parseInt(resultValue[0]);
			containervalue = (null == resultValue[1]) ? "" : resultValue[1];
			hasValue = true;
			nonrepeatableCell.add(containerKey, containervalue);
		}
		
		if ( hasValue ) {
			nonrepeatableCell.sort(new CellComparator.StringComparator<Integer>());
			finalData = nonrepeatableCell.toBytesOnSortedData();
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
