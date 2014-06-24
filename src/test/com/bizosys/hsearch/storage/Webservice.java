/*
* Copyright 2013 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.bizosys.hsearch.storage;


public class Webservice {
	/**
	 *   m|1|[1:7432]|[1:4]|*|[1:758320]|*
	 *  Range Query - [1:7432]
	 *  Match All - *
	 *  Exact match - m
	 */
    
    /**
    public static void main(String[] args) throws Exception {
    
    	HDML.truncate("PriceTable", new NV("Price".getBytes(), "1".getBytes() ));
    	
    	HSearchTablePrice devTable = new HSearchTablePrice();
    	
	    devTable.put((byte)26,23,24,36,38,(byte)18);
    	RecordScalar devRecords = new RecordScalar(
    		"199801011000".getBytes(), new NV("Price".getBytes(), "1".getBytes() , devTable.toBytes() ) ) ;
    	
    	List<RecordScalar> records = new ArrayList<RecordScalar>();
    	records.add(devRecords);
    	
    	HWriter.getInstance(true).insertScalar("PriceTable", records);
    	
    	
    	Client ht = new Client();
        Map<String, String> multiQueryParts = new HashMap<String, String>();
        multiQueryParts.put("Price:1", "*|*|*|*|*|*");
        
        long start = System.currentTimeMillis();
        ht.execute("Price:1", multiQueryParts);
        long end = System.currentTimeMillis();
        System.out.println(" finished in  " + (end - start) + " millis ");	
	}
	*/
}
