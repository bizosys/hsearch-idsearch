/*
* Copyright 2010 Bizosys Technologies Limited
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
package com.bizosys.hsearch.embedded;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.embedded.EmbeddedHSearch;
import com.bizosys.unstructured.SearchConfiguration;

public class EmbeddedSearchExample {
	
    public static void main(String[] args) throws Exception {
    	
    	/**
    	 * Setup configurations - Document Type Codes
    	 */
        SearchConfiguration conf = SearchConfiguration.getInstance();
        Map<String, Integer> docTypes = new HashMap<String, Integer>();
        docTypes.put("www", 1);
        conf.instantiateDocumentTypeCodes(docTypes);
        
    	/**
    	 * Setup configurations - Field Type Codes
    	 */
        Map<String, Integer> fldTypes = new HashMap<String, Integer>();
        fldTypes.put("subject", 1);
        conf.instantiateFieldTypeCodes(fldTypes);
        
    	/**
    	 * Stop setting
    	 */
    	Set<String> stopwords  = new HashSet<String>();
    	stopwords.add("is");
    	stopwords.add("a");
    	
    	/**
    	 * Synonums setting
    	 */
    	Map<String,String> conceptAgainstSynonums  = new HashMap<String, String>();
    	conceptAgainstSynonums.put("bangalore", "bangaluru|bengalore");

    	EmbeddedHSearch engine = new EmbeddedHSearch(stopwords, conceptAgainstSynonums, '|');

    	/**
    	 * Indexing
    	 */
    	Map<Integer, String> docIdWithFieldValue = new HashMap<Integer, String>();
    	docIdWithFieldValue.put(1, "Bangalore is a great city");
    	docIdWithFieldValue.put(2, "Mumbai is a business city");
    	docIdWithFieldValue.put(3, "bangaluru is IT hub");
    	engine.addToIndex("www", "subject", docIdWithFieldValue);
    	
    	engine.commit();

    	{
        	/**
        	 * Searching to find all words
        	 */
        	engine.setTofindAllWords(true);
        	Set<Integer> docIds = engine.searchIds("bangaluru city");
        	if ( null != docIds) System.out.println(docIds.toString());
        	System.out.println("------------------------------");
    	}
    	{
    		
        	/**
        	 * Searching to find any word
        	 */
    		engine.setTofindAllWords(false);
        	Set<Integer> docIds = engine.searchIds("bangaluru city");
        	if ( null != docIds) System.out.println(docIds.toString());
        	System.out.println("------------------------------");
    	}

    	Map<Integer, String> docIdWithFieldValueAppend = new HashMap<Integer, String>();
        docTypes.put("crm", 2);
        conf.instantiateDocumentTypeCodes(docTypes);

    	{
        	/**
        	 * Searching to find by document type, subject type and word
        	 */
        	engine.setTofindAllWords(true);
        	Set<Integer> docIds = engine.searchIds("www/subject:Bangalore");
        	if ( null != docIds) System.out.println(docIds.toString());
        	System.out.println("------------------------------");
    	}    	

    	{
        	/**
        	 * Searching Unknown document type
        	 */
        	engine.setTofindAllWords(true);
        	Set<Integer> docIds = engine.searchIds("crm/subject:Bangalore");
        	if ( null != docIds) System.out.println(docIds.toString());
        	System.out.println("------------------------------");

        	for ( int i=0; i<1000000; i++) {
        		if ( i % 10000 == 0) System.out.println(i);
            	engine.searchIds("crm/subject:Bangalore");
    		}
    	}    	
    	
    	{
        	/**
        	 * Searching with complex query
        	 */
            
        	docIdWithFieldValueAppend.put(4, "Bangalore City Customer List");
        	engine.addToIndex("crm", "subject", docIdWithFieldValueAppend);
        	engine.commit();
        	engine.closeWriter();

        	String multiQuery = "Documents:A AND Documents:B"; //Documents:A AND Findings:B
        	Map<String, String> multiqueryParts = new HashMap<String, String>();
        	multiqueryParts.put("Documents:A", "crm/\\*:bangaluru");
        	multiqueryParts.put("Documents:B", "crm:city");
        	//System.out.print(multiQuery + "\n" + multiqueryParts.toString());
        	Set<Integer> docIds = engine.searchIds(multiQuery, multiqueryParts, "Documents:A", "Documents:B");
        	if ( null != docIds) System.out.println(docIds.toString());

        	engine.close();
        	
    	}

    }
}
