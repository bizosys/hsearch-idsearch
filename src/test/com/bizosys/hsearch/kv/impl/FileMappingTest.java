package com.bizosys.hsearch.kv.impl;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.oneline.ferrari.TestAll;

public class FileMappingTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];
	public FieldMapping fm = null;
	public static void main(String[] args) throws Exception {
		FileMappingTest t = new FileMappingTest();

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
		String schemaPath = "D:/work/shopgirl/conf/schema_import.xml";
		fm = FieldMapping.getInstance(schemaPath);
	}
}