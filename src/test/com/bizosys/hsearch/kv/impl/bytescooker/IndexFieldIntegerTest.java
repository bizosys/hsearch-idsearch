package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.kv.dao.MapperKVBase;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmpty;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVIntegerInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;
import com.bizosys.hsearch.treetable.client.HSearchQuery;

public class IndexFieldIntegerTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws NumberFormatException 
	 */
	public static void main(String[] args) throws IOException, NumberFormatException, ParseException {
		
		//testKVIndexAppend();
		testKVIndexAppendInvertedUncompressed();
		

	}

	private static void testKVIndexAppendInvertedUncompressed() throws IOException, ParseException {
		
		List<Text> values = new ArrayList<Text>();
		StringBuilder sb = new StringBuilder(1024);
		for ( int i=0; i<100; i++) {
			sb.append(i).append(KVIndexer.FIELD_SEPARATOR).append(i);
			values.add(new Text(sb.toString()));
			sb.setLength(0);

			sb.append(i*500).append(KVIndexer.FIELD_SEPARATOR).append(i);
			values.add(new Text(sb.toString()));
			sb.setLength(0);

			sb.append(i*5000).append(KVIndexer.FIELD_SEPARATOR).append(i);
			values.add(new Text(sb.toString()));
			sb.setLength(0);
		}
		
		HSearchTableKVIntegerInverted deserTable = new HSearchTableKVIntegerInverted(false);
		MapperKVBase base = new MapperKVBaseEmpty() {
			
			@Override
			public boolean onRowKey(int id) {
				System.out.println(id);
				return false;
			}
			
			@Override
			public boolean onRowCols(int key, Object value) {
				System.out.println(key + "\t" + value);
				return true;
			}

			@Override
			public boolean onRowKey(BitSetWrapper ids) {
				return false;
			}

			@Override
			public boolean onRowCols(BitSetWrapper ids, Object value) {
				System.out.println(ids.cardinality() + "\t" + value.toString());
				return false;
			}

			@Override
			public void setMergeId(byte[] mergeId) throws IOException {
				
			}
		};

		byte[] serExisting = IndexFieldInteger.cook(values, true, false);
		values.clear();
		
		for ( int i=150; i<153; i++) {
			sb.append(i).append(KVIndexer.FIELD_SEPARATOR).append(i);
			values.add(new Text(sb.toString()));
			sb.setLength(0);
		}
		byte[] serAppended = IndexFieldInteger.cook(values, serExisting, true, false);
		
		
		
		long s = System.currentTimeMillis();
		deserTable.get(serAppended, new HSearchQuery("*|*"), base);		
		long e = System.currentTimeMillis();
		System.out.println("Time taken :" + + (e - s));
	}
	
	private static void testKVIndexAppend() throws IOException, ParseException {
		List<Text> values = new ArrayList<Text>();
		StringBuilder sb = new StringBuilder(1024);
		for ( int i=0; i<100; i++) {
			sb.append(i).append(KVIndexer.FIELD_SEPARATOR).append(i);
			values.add(new Text(sb.toString()));
			sb.setLength(0);
		}
		
		HSearchTableKVInteger deserTable = new HSearchTableKVInteger();
		MapperKVBase base = new MapperKVBaseEmpty() {
			
			@Override
			public boolean onRowKey(int id) {
				System.out.println(id);
				return false;
			}
			
			@Override
			public boolean onRowCols(int key, Object value) {
				System.out.println(key + "\t" + value);
				return true;
			}

			@Override
			public boolean onRowKey(BitSetWrapper ids) {
				return false;
			}

			@Override
			public boolean onRowCols(BitSetWrapper ids, Object value) {
				return false;
			}

			@Override
			public void setMergeId(byte[] mergeId) throws IOException {
				
			}
		};

		byte[] serExisting = IndexFieldInteger.cook(values, false, false);
		values.clear();
		
		for ( int i=150; i<153; i++) {
			sb.append(i).append(KVIndexer.FIELD_SEPARATOR).append(i);
			values.add(new Text(sb.toString()));
			sb.setLength(0);
		}
		byte[] serAppended = IndexFieldInteger.cook(values, serExisting, false, false);
		
		
		
		long s = System.currentTimeMillis();
		deserTable.get(serAppended, new HSearchQuery("*|*"), base);		
		long e = System.currentTimeMillis();
		System.out.println("Time taken :" + + (e - s));
	}

}
