package com.bizosys.hsearch.kv;

import java.util.Set;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.oneline.ferrari.TestAll;

public class TestMergeIdsSearch extends TestCase {

	public static String[] modes = new String[] {"all", "random", "method"};
	public static String mode = modes[1];
	public FieldMapping fm = null;
	public static void main(final String[] args) throws Exception {
		TestMergeIdsSearch t = new TestMergeIdsSearch();

		if ( modes[0].equals(mode) ) {
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) {
			String status = TestFerrari.testRandom(t).getFailedFunctions();
			IdSearchLog.l.fatal("Failed :" + status);

		} else if  ( modes[2].equals(mode) ) {
			t.setUp();
			t.testSearchWithRegex();
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
	
	public final void testGetMergeIds() throws Exception {
		Searcher searcher = new Searcher("test", fm);
		Set<String> mergeIds = searcher.getMergeIds();
		assertEquals(3, mergeIds.size());
	}

	public final void testSearchWithListOfIds() throws Exception {
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		Set<String> mergeIds = searcher.getMergeIds();
		IEnricher enricher = null;
		searcher.search(mergeIds, "age,location,marks","age:25", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();
		assertEquals(20, mergedResult.size());
	}

	public final void testSearchWithRegex() throws Exception {
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new ExamResult();
		IEnricher enricher = null;
		searcher.searchRegex("A.*","age,location,marks","age:25", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();
		assertEquals(10, mergedResult.size());
	}

}
