package com.bizosys.hsearch.kv.dao.inverted;

import java.io.IOException;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.MapperKVBase;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmpty;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.oneline.ferrari.TestAll;

public class HSearchTableKVShortInvertedTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];
	public FieldMapping fm = null;
	public static void main(String[] args) throws Exception {
		HSearchTableKVShortInvertedTest t = new HSearchTableKVShortInvertedTest();

		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
			TestFerrari.testRandom(t);

		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.sanityTest();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
	}

	@Override
	protected void tearDown() throws Exception {
		System.exit(1);
	}

	public void sanityTest() throws Exception {
		boolean isCompressed = true;
		
		HSearchTableKVShortInverted table = new HSearchTableKVShortInverted(isCompressed);
		
		for ( int i=0; i<Short.MAX_VALUE; i++) {
			table.put(i%100, (short)i);
		}
		table.put(10, (short)-90);
		table.put(20, (short)-1006);
		
		byte[] ser = table.toBytes();
		float size = ser.length/1024/1024;
		System.out.println("Data Size :" + size + " MB" + " or " + ser.length + " bytes");
		
		HSearchTableKVShortInverted deserTable = new HSearchTableKVShortInverted(isCompressed);
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
				System.out.println(ids.toString() + "\t" + value.toString());
				return false;
			}

			@Override
			public void setMergeId(byte[] mergeId) throws IOException {
				
			}
		};
		
		long s = System.currentTimeMillis();
//		deserTable.get(ser, new HSearchQuery("*|23"), base);		
//		deserTable.get(ser, new HSearchQuery("*|[23:45]"), base);
		System.out.println("\n*****************\n");
		deserTable.get(ser, new HSearchQuery("*|-1006"), base);
		long e = System.currentTimeMillis();
		System.out.println("Time taken :" + + (e - s));
			
	}
	

}