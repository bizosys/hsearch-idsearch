package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.text.ParseException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.idsearch.admin.ColGenerator;
import com.bizosys.hsearch.kv.impl.FieldMapping;


public class Test {
	
	public static void main(String[] args) throws ParseException, InterruptedException, NumberFormatException, IOException, ExecutionException, FederatedSearchException{
		//index();
		long start = System.currentTimeMillis();
		String schemaPath = "hsearch-idsearch/src/test/com/bizosys/hsearch/kv/testwell.xml";
		FieldMapping fm = FieldMapping.getInstance(schemaPath);
		
		Searcher searcher = new Searcher("test", fm);
		KVRowI aBlankRow = new Wellbore();

		IEnricher enricher = null;
		searcher.search("Wellborep1", "welboreid,readingvalue","category:A", aBlankRow, enricher);
		Set<KVRowI> mergedResult = searcher.getResult();

		long end = System.currentTimeMillis();
		//assertEquals(10, mergedResult.size());
		int count = mergedResult.size();
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		
		int reading = 0;
		for (KVRowI kvRowI : mergedResult) {
			Wellbore bore = (Wellbore) kvRowI;
			reading = bore.readingvalue;
			if(reading < min)
				min = bore.readingvalue;
			if(reading > max)
				max = reading;
		}
		System.out.println("\nMin : "   + min );
		System.out.println("Max : "   + max );
		System.out.println("Count : " + count );
		System.out.println("Time taken " + (end - start) + " ms");

	}

	private static void index() throws ParseException, InterruptedException {
		int[] wells = new int[]{1,2,3,4,5};
		int wellI = 0;
		String[] category = new String[]{"A","B","C"};
		int catI = 0;
		int[] readingType = new int[400];
		int readingI = 0;
		
		for(int i = 0; i < 400; i++){
			readingType[i] = i;
		}
		char FIELD_SEPARATOR = '\t';
		char RECORD_SEPARATOR = '\n';

		StringBuilder sb = new StringBuilder(65534);
		for(int i = 0; i < 100; i++){
			sb.append(wells[wellI]).append(FIELD_SEPARATOR).append(category[catI]).append(FIELD_SEPARATOR).append(readingType[readingI]).append(FIELD_SEPARATOR).append(i).append(FIELD_SEPARATOR).append(i % 1000).append(RECORD_SEPARATOR);
			wellI++;
	    	if ( wellI > 4) 
	    		wellI = 0;
	    	catI++;
	    	if ( catI > 2) 
	    		catI = 0;
	    	readingI++;
	    	if ( readingI > 99) 
	    		readingI = 0;
		}
		
    	String schemaPath = "hsearch-idsearch/src/test/com/bizosys/hsearch/kv/testwell.xml";
    	String valueObjectPath = "hsearch-idsearch/src/test/com/bizosys/hsearch/kv/";
    	String valueObjectClassName = "com.bizosys.hsearch.kv.Wellbore";
    	FieldMapping fm = FieldMapping.getInstance(schemaPath);
		ExamResultDataLoader.index(schemaPath, sb);
    	ColGenerator.createVO(fm, valueObjectPath, valueObjectClassName);
	}
}
