package com.bizosys.unstructured;

import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.idsearch.storage.Client;

public class TestHSearchClient  {
	
	public static void main(String[] args) throws Exception {
		
		Map<String, Float> output = new HashMap<String, Float>();
	  	Client ht = new Client(output);
        Map<String, String> multiQueryParts = new HashMap<String, String>();
        int hashCode = "abinash".hashCode();
        multiQueryParts.put("Documents:1", "*|*|*|*|*|*");
        
        long start = System.currentTimeMillis();
        ht.execute("Documents:1", multiQueryParts);
        long end = System.currentTimeMillis();
        System.out.println(" finished in  " + (end - start) + " millis ");			
	}
	
}
