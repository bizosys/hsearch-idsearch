package com.bizosys.hsearch.kv;

import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.unstructured.util.IdSearchLog;
import com.oneline.ferrari.TestAll;

public class ExamResultSearch extends TestCase {

	public static String[] modes = new String[] {"all", "random", "method"};
	public static String mode = modes[1];
	public FieldMapping fm = null;
	public static void main(final String[] args) throws Exception {
		ExamResultSearch t = new ExamResultSearch();

		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
			String status = TestFerrari.testRandom(t).getFailedFunctions();
			IdSearchLog.l.fatal("Failed :" + status);

		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			
			t.testFreeTextSearch();
			/**
			t.testRepeatable();
			t.testMultipleFilters();
			t.testFacet();
			t.testPivotFacet();
			t.testSorting();

			t.testFreeTextStored();
			t.testFreeTextNotStored();
			*/
			
			/*
			t.testFreeTextWithNumericals();
			t.testFreeTextNotStored();
			t.testFreeTextNotStored();
			t.testFreeTextNotStored();
			t.testComboSearchWithfacet();
			t.testFreeTextSearch();
			t.testFreeTextStored();
			*/
			
			t.tearDown();
		}
	}

	@Override
	protected final void setUp() throws Exception {
		String schemaPath = "src/test/com/bizosys/hsearch/kv/examresult.xml";
		fm = FieldMapping.getInstance(schemaPath);
	}

	@Override
	protected final void tearDown() throws Exception {
		System.exit(1);
	}

	public final void testSanity() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks","age:25", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		assertEquals(10, mergedResult.size());
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}
	
	public final void testRepeatable() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "sex,age","age:22", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		assertEquals(10, mergedResult.size());
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testMultipleFilters() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks","age:25 AND location:{Hebbal,HSR Layout} AND marks:!6.8", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();
		assertEquals(9, mergedResult.size());
		long end = System.currentTimeMillis();
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testSorting() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "sex,age,location,marks",null, aBlankRow, enricher);
		searcher.sort("sex","^age","^location","^marks");
		Set<KVRowI> mergedResult = searcher.getResult();
		assertEquals(50, mergedResult.size());
		KVRowI firstRow = mergedResult.iterator().next();
		String expected = "Male 22 HSR Layout 0.0";
		String actual = firstRow.getValue("sex") + " " +firstRow.getValue("age") + " " +firstRow.getValue("location") + " " +firstRow.getValue("marks");
		assertEquals(expected, actual);
		long end = System.currentTimeMillis();
//		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testComboSearchWithfacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		BitSetOrSet foundIds = searcher.getIds(fm.tableName, "A", "location:{Hebbal,HSR Layout} AND marks:!2.0");
		System.out.println("Total Ids Found :" + foundIds.size());
		System.out.println("Ids :" + foundIds.toString());
		assertEquals(49, foundIds.size());

		searcher.search(fm.tableName, "A", "age,location,marks","location:{Hebbal}", aBlankRow, enricher);
		searcher.sort("location","marks");
		Map<String, Map<Object, FacetCount>> facets = searcher.createFacetCount("age,location");
		Map<Object, FacetCount> countsLoc = facets.get("location");
		assertEquals(25, countsLoc.get("Hebbal").count);
		Map<Object, FacetCount> countsAge = facets.get("age");
		assertEquals(5, countsAge.get(23).count);
		long end = System.currentTimeMillis();
		System.out.println("Facetted results in " + (end - start) + " ms.");
	}

	public final void testFacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		Map<String, Set<Object>> facetResult = searcher.facet(fm.tableName, "B", "age,marks,location", "location:BTM Layout", aBlankRow);
		long end = System.currentTimeMillis();
		assertEquals(25, facetResult.get("marks").size());
		assertEquals(1, facetResult.get("location").size());
		assertEquals(5, facetResult.get("age").size());
		//System.out.println("Fetched " + facetResult.size() + "facet sets in " + (end - start) + " ms.");
	}

	public final void testPivotFacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		Map<String, List<HsearchFacet>> facetResult = searcher.pivotFacet(fm.tableName, "B", "age,role|age,marks", "location:BTM Layout", aBlankRow);
		long end = System.currentTimeMillis();

		int size = facetResult.get("age,marks").size();
		assertEquals(5, size);
		//System.out.println("Fetched " + facetResult.size() + " facet results in " + (end - start) + " ms.");
	}

	public final void testFreeTextSearch() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		searcher.setCheckForAllWords(true);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks","remarks:authentic AND age:25", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();
		long end = System.currentTimeMillis();

		assertEquals(3, mergedResult.size());
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testFreeTextStored() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "empid,age,commentsVal","comments:Tremendous", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		assertEquals(16, mergedResult.size());
		for (KVRowI kvRowI : mergedResult) {
			assertNotNull(kvRowI.getValue("empid"));
			assertNotNull(kvRowI.getValue("age"));
			assertNotNull(kvRowI.getValue("commentsVal"));
			assertTrue(kvRowI.getValue("commentsVal").toString().length() > 0);
			assertEquals(kvRowI.getValue("commentsVal").toString(), "Tremendous boy");
			assertTrue( (Integer) kvRowI.getValue("empid") > 0);
			assertTrue( (Integer) kvRowI.getValue("age") > 0);
		}
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}
	

	public final void testFreeTextNotStored() throws Exception  {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,remarks","remarks:fabulous", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		assertEquals(16, mergedResult.size());
		for (KVRowI kvRowI : mergedResult) {
			assertNotNull(kvRowI.getValue("age"));
			assertTrue( (Integer) kvRowI.getValue("age") > 0 );
		}
	}

	public final void testFreeTextWithNumericals() throws Exception {
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks,comments","comments:Tremendous", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();

		assertEquals(16, mergedResult.size());
	}	
	
	public final void testNullQuery() throws Exception {
		Searcher searcher = new Searcher(fm.tableName, fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age","remarks:study", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		assertEquals(16, mergedResult.size());
	}	
	
	public final void testPhraseQuery() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		searcher.setCheckForAllWords(true);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "commentsVal","comments:'tremendous boy'", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			assertNotNull(kvRowI.getValue("commentsVal"));
			assertTrue(kvRowI.getValue("commentsVal").toString().length() > 0);
			assertEquals(kvRowI.getValue("commentsVal").toString(), "Tremendous boy");
		}
		assertEquals(16, mergedResult.size());
	}		

	public final void testUnknownWord() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "commentsVal","comments:'unknown'", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			System.out.println(kvRowI.getId() + "\t" +  kvRowI.getValue("commentsVal"));
		}
		assertEquals(0, mergedResult.size());
	}		

	public final void testUnknownKnownWord() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		searcher.setCheckForAllWords(true);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "commentsVal","comments:'unknown boy'", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			System.out.println(kvRowI.getId() + "\t" +  kvRowI.getValue("commentsVal"));
		}
		assertEquals(0, mergedResult.size());
	}		

	public final void testLemos() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "commentsVal","comments:boys", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			assertTrue(kvRowI.getValue("commentsVal").toString().indexOf("boy") >= 0 );
		}
		assertEquals(33, mergedResult.size());
	}		

}