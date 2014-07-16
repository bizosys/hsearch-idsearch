package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.util.HSearchConfig;
import com.oneline.ferrari.TestAll;

public class ExamResultSearch extends TestCase {

	public static String[] modes = new String[] {"all", "random", "method"};
	public static String mode = modes[2];
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
			t.testPhraseQuery();
			//t.testMultipleFilters();
			//t.testSorting();
			//t.testFacet();
			//t.testMultiThreadedFacet();
			//t.testSkewPoint();
			//t.testFreeTextNotStored();
			//t.testFreeTextSearch();
			//t.testPivotFacet();
			//t.testFacet();
			//t.testFreeTextStored();

			
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
		String schemaPath = "hsearch-idsearch/src/test/com/bizosys/hsearch/kv/examresult.xml";
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
		searcher.search("A", "age,location,role,marks","location:HSR\\+Layout", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		//assertEquals(10, mergedResult.size());
		for (KVRowI kvRowI : mergedResult) {
			System.out.println(kvRowI.getValue("location"));
		}
		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testSkewPoint() throws Exception {
		if ( 1 == 1 ) return;
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		searcher.searchSkew("A", "age,location,role,marks","location:HSR\\+Layout", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		//assertEquals(10, mergedResult.size());
		for (KVRowI kvRowI : mergedResult) {
			System.out.println(kvRowI.getValue("location"));
		}
		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testRepeatable() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		searcher.search("A", "sex,age","age:22", aBlankRow, enricher);
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
		searcher.search( "A", "age,location,marks","age:24 AND location:{HSR+Layout,Hebbal} AND marks:!6.8", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();
		for (KVRowI kvRowI : mergedResult) {
			//System.out.println(kvRowI.getValue("age") + "\t" + kvRowI.getValue("marks"));
		}
		assertEquals(10, mergedResult.size());
		long end = System.currentTimeMillis();
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testSorting() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search("A", "sex,age,location,marks","age:*", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();
		assertEquals(50, mergedResult.size());
		searcher.sort("sex","^age","^location","^marks");
		mergedResult = searcher.getResult();
		assertEquals(50, mergedResult.size());		
		KVRowI firstRow = mergedResult.iterator().next();
		String expected = "Male 22 HSR+Layout 0.0";
		String actual = firstRow.getValue("sex") + " " +firstRow.getValue("age") + " " +firstRow.getValue("location") + " " +firstRow.getValue("marks");
		assertEquals(expected, actual);
		long end = System.currentTimeMillis();
		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public final void testComboSearchWithFacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		BitSetOrSet foundIds = searcher.getIds("A", "location:*");
		BitSetWrapper matchedIds = foundIds.getDocumentSequences();
		Map<String, Map<Object, FacetCount>> facets = searcher.createFacetCount(matchedIds, "A", "marks,age,location");
		Map<Object, FacetCount> countsLoc = facets.get("location");
		//assertEquals(25, countsLoc.get("Hebbal").count);
		Map<Object, FacetCount> countsAge = facets.get("age");
		//assertEquals(5, countsAge.get(23).count);
		
		long end = System.currentTimeMillis();
		System.out.println("Facetted results in " + (end - start) + " ms. " + facets);
	}

	public final void testFacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		SearcherPluginTest plugin = new SearcherPluginTest(); 

		searcher.setPlugin(plugin);
		searcher.search("B", "age,marks,location", "location:BTM Layout", "age,marks,location", aBlankRow);

		long end = System.currentTimeMillis();
		
		assertEquals(25, plugin.facetResult.get("marks").size());
		assertEquals(1, plugin.facetResult.get("location").size());
		assertEquals(5, plugin.facetResult.get("age").size());
	}

	public final void testMultiThreadedFacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		SearcherPluginTest plugin = new SearcherPluginTest(); 
		
		HSearchConfig.getInstance().getConfiguration().set("hsearch.facet.multithread.enabled", true);
		searcher.setPlugin(plugin);
		searcher.search("B", "age,marks,location", "location:BTM Layout", "age,marks,location", aBlankRow);
		
		long end = System.currentTimeMillis();
		
		assertEquals(25, plugin.facetResult.get("marks").size());
		assertEquals(1, plugin.facetResult.get("location").size());
		assertEquals(5, plugin.facetResult.get("age").size());
	}

	public final void testPivotFacet() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();

		Map<String, List<HsearchFacet>> facetResult = searcher.pivotFacet("B", "age,role|age,marks", "location:BTM Layout", aBlankRow);
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
		searcher.search("A", "age,location,marks","remarks:authentic AND age:25", aBlankRow, enricher);
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
		searcher.search("A", "empid,age,commentsVal","comments:Tremendous", aBlankRow, enricher);
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
			assertTrue( (Byte) kvRowI.getValue("age") > 0);
		}
		//System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}
	

	public final void testFreeTextNotStored() throws Exception  {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search("A", "age,remarks","remarks:fabulous", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		assertEquals(16, mergedResult.size());
		for (KVRowI kvRowI : mergedResult) {
			assertNotNull(kvRowI.getValue("age"));
			assertTrue( (Byte) kvRowI.getValue("age") > 0 );
		}
	}

	public final void testFreeTextWithNumericals() throws Exception {
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search("A", "age,location,marks,comments","comments:Tremendous", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();

		assertEquals(16, mergedResult.size());
	}	
	
	public final void testNullQuery() throws Exception {
		Searcher searcher = new Searcher(fm.tableName, fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search("A", "age","remarks:study", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		assertEquals(16, mergedResult.size());
	}	
	
	public final void testPhraseQuery() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		searcher.setCheckForAllWords(false);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		
		searcher.setInternalFetchLimit(100);
		//searcher.setPage(25, 20);
		searcher.search("B", "commentsVal","comments:'good boy'", aBlankRow, enricher);
		
		searcher.setPlugin(new ISearcherPlugin() {
			
			@Override
			public void onJoin(String mergeId, BitSetWrapper foundIds, Map<String, QueryPart> whereParts,
					Map<Integer, KVRowI> foundIdWithValueObjects) {
			}
			
			@Override
			public void onFacets(String mergeId,Map<String, Map<Object, FacetCount>> facets) {}
			
			@Override
			public void beforeSelectOnSorted(String mergeId, BitSetWrapper foundIds) {}
			
			@Override
			public void beforeSelect(String mergeId, BitSetWrapper foundIds) {}
			
			@Override
			public void afterSort(String mergeId, Set<KVRowI> resultSet) {}
			
			@Override
			public void afterSelectOnSorted(String mergeId, BitSetWrapper foundIds, Set<KVRowI> resultset) {}
			

			@Override
			public boolean beforeSort(String mergeId, String sortFields,
					Set<KVRowI> resultSet,
					Map<Integer, BitSetWrapper> internalRanks) {
				
				for (int i=0; i<internalRanks.size(); i++) {
					System.out.println("NNN:" + i + "\t" + internalRanks.get(i).toString());
				}
				return false;
			}

			@Override
			public void afterSelect(String mergeId, BitSetWrapper foundIds,
					Set<KVRowI> resultset) throws IOException {
				// TODO Auto-generated method stub
				
			}
		});
		
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			assertNotNull(kvRowI.getValue("commentsVal"));
			assertTrue(kvRowI.getValue("commentsVal").toString().length() > 0);
			System.out.println(kvRowI.getValue("commentsVal").toString());
			//assertEquals(kvRowI.getValue("commentsVal").toString(), "Tremendous boy");
		}
		//assertEquals(16, mergedResult.size());
	}		

	public final void testUnknownWord() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search("A", "commentsVal","comments:'unknown'", aBlankRow, enricher);
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
		searcher.search("A", "commentsVal","comments:'unknown boy'", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			System.out.println(kvRowI.getId() + "\t" +  kvRowI.getValue("commentsVal"));
		}
		assertEquals(0, mergedResult.size());
	}		

	/**
	public final void testLemos() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher(fm.tableName, fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search("A", "commentsVal","comments:boys", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		for (KVRowI kvRowI : mergedResult) {
			assertTrue(kvRowI.getValue("commentsVal").toString().indexOf("boy") >= 0 );
		}
		assertEquals(33, mergedResult.size());
	}	
	*/	

}