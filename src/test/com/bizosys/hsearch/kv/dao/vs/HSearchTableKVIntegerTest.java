package com.bizosys.hsearch.kv.dao.vs;

import java.io.IOException;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmpty;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.oneline.ferrari.TestAll;

public class HSearchTableKVIntegerTest  extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];

	public static void main(String[] args) throws Exception {
		
		HSearchTableKVIntegerTest t = new HSearchTableKVIntegerTest();

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
		HSearchTableKVInteger integerTable = new HSearchTableKVInteger();
		integerTable.put(1, -23);
		integerTable.put(2, -29);
		integerTable.put(3, 23);
		integerTable.put(4, 23);
		for(int i = 10; i < 1000; i++){
			integerTable.put(i, i);
		}
		byte[] data = integerTable.toBytes();
//		shortTable.parse(data, new Cell2Visitor<Integer, Short>() {
//			
//			@Override
//			public void visit(Integer k, Short v) {
//				System.out.println(k + "\t" + v);
//			}
//		});
		
		HSearchQuery hq = new HSearchQuery("*|-23");
		
		integerTable.get(data, hq, new MapperKVBaseEmpty() {
			
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
				System.out.print("\n" + key + "\t" + Integer.parseInt(value.toString()));
				return false;
			}
		});
	}

}
