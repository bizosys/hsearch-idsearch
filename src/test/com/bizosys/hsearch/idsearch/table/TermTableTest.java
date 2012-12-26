package com.bizosys.hsearch.idsearch.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.byteutils.ByteStringTest;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.idsearch.table.TermTableRow;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.oneline.ferrari.TestAll;

public class TermTableTest extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
		public static String mode = modes[2];  
		
		public static void main(String[] args) throws Exception {
			TermTableTest t = new TermTableTest();
			
			if ( modes[0].equals(mode) ) {
				TestAll.run(new TestCase[]{t});
			} else if  ( modes[1].equals(mode) ) {
		        TestFerrari.testRandom(t);
		        
			} else if  ( modes[2].equals(mode) ) {
				t.setUp();
				t.testTsf();
				t.tearDown();
			}
		}

		@Override
		protected void setUp() throws Exception {
		}
		
		@Override
		protected void tearDown() throws Exception {
		}
		

		public void testTsf() throws Exception
		{
			TermTable tt = new TermTable();
			File searchFile = new File("f:\\work\\hsearch-idsearch\\reference\\searchdata.tsv");
			BufferedReader reader = null;
			InputStream stream = null;

			stream = new FileInputStream(searchFile); 
			reader = new BufferedReader ( new InputStreamReader(stream) );
			
			String line;
			TermTableRow searchData;
			while((line=reader.readLine())!=null)
			{
				String[] lineVars = line.split("\t");
				searchData = new TermTableRow();
				searchData.setParams(lineVars[0], Integer.parseInt(lineVars[1]), Integer.parseInt(lineVars[2]), 
						Integer.parseInt(lineVars[3]), Float.parseFloat(lineVars[4]));
				tt.addSearchData(searchData);
			}

			TermQuery filter = new TermQuery();
			filter.setWordRecordtypeFieldType("searchField2", 1, 2);
			byte[] cellBytes = tt.toBytes();
			
			Cell2<Integer, Float> recordIds = 
					new Cell2<Integer, Float>(SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
			Cell4<Integer, Integer, Integer, Float> records = 
					new Cell4<Integer, Integer, Integer, Float>(SortedBytesInteger.getInstance(), 
							SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
							SortedBytesFloat.getInstance());
			
			tt.findIdsFromSerializedTableQuery(cellBytes, filter, recordIds, records);
			
	
			for (CellKeyValue<Integer, Float> ckv : recordIds.getMap())
			{
				System.out.println("Final Output :" + ckv.getKey() +", " +ckv.getValue());
			}
		}

}
