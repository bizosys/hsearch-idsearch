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

public class HSearchTableKVBooleanInvertedTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[1];
	public FieldMapping fm = null;
	public static void main(String[] args) throws Exception {
		HSearchTableKVBooleanInvertedTest t = new HSearchTableKVBooleanInvertedTest();

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
		String schemaPath = "src/test/com/bizosys/hsearch/kv/examresult.xml";
		fm = FieldMapping.getInstance();
		fm.parseXML(schemaPath);
	}

	@Override
	protected void tearDown() throws Exception {
		System.exit(1);
	}

	public void sanityTest() throws Exception {
		HSearchTableKVBooleanInverted table = new HSearchTableKVBooleanInverted(false);
		
		for ( int i=0; i<100000000; i++) {
			if ( i == 100) table.put(i, true);
			else table.put(i, false);
		}
		byte[] ser = table.toBytes();
		float size = ser.length/1024/1024;
		System.out.println("Data Size :" + size + " MB" + " or " + ser.length + " bytes");
		
		HSearchTableKVBooleanInverted deserTable = new HSearchTableKVBooleanInverted(true);
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
				// TODO Auto-generated method stub
				
			}
		};
		
		long s = System.currentTimeMillis();
		deserTable.get(ser, new HSearchQuery("*|true"), base);		
		long e = System.currentTimeMillis();
		System.out.println("Time taken :" + + (e - s));
			
	}
	

}