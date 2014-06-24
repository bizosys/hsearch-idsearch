package com.bizosys.hsearch.kv.dao.vs;

import java.io.IOException;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmpty;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.oneline.ferrari.TestAll;

public class HSearchTableVIntegerTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];

	public static void main(String[] args) throws Exception {
		
		HSearchTableVIntegerTest t = new HSearchTableVIntegerTest();

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
		HSearchTableVInteger table = new HSearchTableVInteger(Integer.MIN_VALUE);
		table.put(1, 5);
		table.put(2, 10);
		table.put(4, 15);
		table.put(8, 20);
		table.put(100, 25);
		
		byte[] data = table.toBytes();
		
		table.get(data, new HSearchQuery("*|*"), new MapperKVBaseEmpty() {
			
			@Override
			public void setMergeId(byte[] arg0) throws IOException {
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
				System.out.println(key + "\t" + value.toString());
				return false;
			}
		});
	}
}
