package com.bizosys.hsearch.kv;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.IEnricher;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.oneline.ferrari.TestAll;

public class SearchTester extends TestCase {

	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[1];
	public FieldMapping fm = null;
	public static void main(String[] args) throws Exception {
		SearchTester t = new SearchTester();

		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
			TestFerrari.testRandom(t);

		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.freeTextSearchTest();
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

	public void searchTest() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm, null);
		KVRowI aBlankRow = new ExamResult();

		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks","age:25 AND location:{Hebbal,HSR Layout} AND marks:!2.0", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		if(mergedResult.size() == 0){
			System.out.println("No data for given query.");
		}else{
			for (KVRowI aResult : mergedResult) {
				System.out.println(aResult.getId() + "\t" + aResult.getValue("age")+ "\t" + aResult.getValue("location")+ "\t" + aResult.getValue("marks"));
			}
		}
		
		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}
	
	public void facetTest() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm, null);
		KVRowI aBlankRow = new ExamResult();
		
		Map<String, Set<Object>> facetResult = searcher.facet(fm.tableName, "B", "age,marks,location", "location:BTM Layout", aBlankRow);
		long end = System.currentTimeMillis();
		if(facetResult.size() == 0){
			System.out.println("No data for given query.");
		}else{
			for (String facetField : facetResult.keySet()) {
				System.out.println(facetField + " ==> " + facetResult.get(facetField));
			}
		}
		
		System.out.println("Fetched " + facetResult.size() + " results in " + (end - start) + " ms.");
	}
	
	public void pivotFacetTest() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm, null);
		KVRowI aBlankRow = new ExamResult();
		
		Map<String, List<HsearchFacet>> facetResult = searcher.pivotFacet(fm.tableName, "B", "age,role|age,marks", null, aBlankRow);
		long end = System.currentTimeMillis();
		if(facetResult.size() == 0){
			System.out.println("No data for given query.");
		}else{
			for (String pivotField : facetResult.keySet()) {
				System.out.println(pivotField);
				List<HsearchFacet> hsearchFacets = facetResult.get(pivotField);
				for (HsearchFacet hsearchFacet : hsearchFacets) {
					System.out.println("\t" + hsearchFacet.getField() + " => " + hsearchFacet.getValue().toString());
					System.out.println("\t\t" + hsearchFacet.getinternalFacets().toString());
				}
			}
		}
		
		System.out.println("Fetched " + facetResult.size() + " results in " + (end - start) + " ms.");
	}

	public void freeTextSearchTest() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm, new StandardAnalyzer(Version.LUCENE_36));
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks","remarks:authentic AND age:25", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		if(mergedResult.size() == 0){
			System.out.println("No data for given query.");
		}else{
			for (KVRowI aResult : mergedResult) {
				System.out.println(aResult.getId() + "\t" + aResult.getValue("age")+ "\t" + aResult.getValue("location")+ "\t" + aResult.getValue("marks"));
			}
		}
		
		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}

	public void freeTextStoredSearchTest() throws Exception {
		long start = System.currentTimeMillis();
		Searcher searcher = new Searcher("test", fm, new StandardAnalyzer(Version.LUCENE_36));
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.search(fm.tableName, "A", "age,location,marks,comments","comments:Tremendous", aBlankRow, enricher);
		searcher.sort("location","marks");
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		if(mergedResult.size() == 0){
			System.out.println("No data for given query.");
		}else{
			for (KVRowI aResult : mergedResult) {
				System.out.println(aResult.getId() + "\t" + aResult.getValue("age")+ "\t" + aResult.getValue("location")+ "\t" + aResult.getValue("marks")+ "\t" + aResult.getValue("comments"));
			}
		}
		
		System.out.println("Fetched " + mergedResult.size() + " results in " + (end - start) + " ms.");
	}
	

}