package com.bizosys.hsearch.kv.dao.vs;

import java.io.IOException;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmpty;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVShort;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.oneline.ferrari.TestAll;

public class HSearchTableKVShortTest  extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];

	public static void main(String[] args) throws Exception {
		
		HSearchTableKVShortTest t = new HSearchTableKVShortTest();

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
		HSearchTableKVShort shortTable = new HSearchTableKVShort();
		shortTable.put(1, (short)-23);
		shortTable.put(2, (short)-70);
		shortTable.put(3, (short)23);
		shortTable.put(4, (short)89);
		byte[] data = shortTable.toBytes();
		
		HSearchQuery hq = new HSearchQuery("*|[-70 : 89]");
		shortTable.get(data, hq, new MapperKVBaseEmpty() {
			
			@Override
			public void setMergeId(byte[] mergeId) throws IOException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public boolean onRowKey(BitSetWrapper ids) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean onRowCols(BitSetWrapper ids, Object value) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean onRowKey(int id) {
				// TODO Auto-generated method stub
				return false;
			}
			
			@Override
			public boolean onRowCols(int key, Object value) {
				System.out.print("\n" + key + "\t" + Short.parseShort(value.toString()));
				return false;
			}
		});
	}

}
