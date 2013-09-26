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
package com.bizosys.hsearch.embedded;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.embedded.donotmodify.HSearchTableDocuments;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.unstructured.AnalyzerFactory;
import com.bizosys.unstructured.IndexWriter;
import com.sun.tools.corba.se.idl.InvalidArgument;

public class EmbeddedUtil {
	
	IndexWriter writer = null;

	public void addToIndex(Analyzer analyzer, String docType, String fieldType, Map<Integer, String> docIdWithFieldValue) throws IOException, InstantiationException, InvalidArgument {
		
		if ( null == writer) writer = new IndexWriter(new HSearchTableDocuments());
    	AnalyzerFactory analyzers = AnalyzerFactory.getInstance().setDefault(analyzer);
    	
		for (Integer docId : docIdWithFieldValue.keySet()) {
		    Document lucenDoc = new Document();
		    lucenDoc.add(new Field(fieldType, docIdWithFieldValue.get(docId), Field.Store.YES, Field.Index.ANALYZED));
		    writer.addDocument(docId, lucenDoc,docType, analyzers);
		}
	}
    
    public byte[] toBytes() throws NullPointerException, IOException {
		if ( null == writer) throw new NullPointerException("Null data to index. addToIndex first");
		byte[] data = writer.toBytes();
    	return data;
    }
    
    public BitSetOrSet search(byte[] data, Analyzer analyzer, String  docType, String fieldType, String query) throws IOException, ParseException, InstantiationException {
    	
    	
    	String docTypeCode = "*".equals(docType) ? "*" :
    		new Integer(DocumentTypeCodes.getInstance().getCode(docType)).toString();
    	
    	String fldTypeCode = "*".equals(fieldType) ? "*" :
    		new Integer(FieldTypeCodes.getInstance().getCode(fieldType)).toString();

		QueryParser qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
		Query q = null;
		try {
			q = qp.parse(query);
		} catch ( org.apache.lucene.queryParser.ParseException ex) {
			throw new ParseException(ex.getMessage(), 0);
		}
		Set<Term> terms = new HashSet<Term>();
		q.extractTerms(terms);

		StringBuilder allWords = null;
		for (Term term : terms) {
			String fieldText = term.text();
			if ( null == allWords) {
				allWords = new StringBuilder("{");
				allWords.append(Hashing.hash(fieldText));
			} else {
				allWords.append(',').append(Hashing.hash(fieldText));
			}
		}
		allWords.append('}');

		StringBuilder queryBuilder = new StringBuilder(1024);
		queryBuilder.append(docTypeCode);
		queryBuilder.append('|');
		queryBuilder.append(fldTypeCode);
		queryBuilder.append('|');
		queryBuilder.append(allWords.toString());
		queryBuilder.append("|*|*");
		
    	HSearchQuery hq = new HSearchQuery(queryBuilder.toString());
		return search(data, hq);
    }
    
    public BitSetOrSet search(byte[] data, HSearchQuery hq) throws IOException {
		MapperDocuments md  = new MapperDocuments();
		HSearchTableDocuments htd = new HSearchTableDocuments();
		htd.get(data, hq, md);
		return md.getUniqueMatchingDocumentIds();
    }
    
    public void addDoumentTypes(Map<String, Integer> dtypes) throws IOException{
		DocumentTypeCodes.instanciate( DocumentTypeCodes.builder().add(dtypes).toBytes() );
    }
    
    public void addFieldTypes(Map<String, Integer> ftypes) throws IOException{
    	FieldTypeCodes.instanciate( FieldTypeCodes.builder().add(ftypes).toBytes() );
    }
    
    public void close() throws IOException {
    	if ( null != this.writer ) this.writer.close();
    }

    public static void main(String[] args) throws IOException, InstantiationException, ParseException, InvalidArgument {

		EmbeddedUtil indexer = new EmbeddedUtil();
    	
    	Map<Integer, String> docIdWithFieldValue1 = new HashMap<Integer, String>();
    	docIdWithFieldValue1.put(1, "Abinash");
    	docIdWithFieldValue1.put(2, "Subhendu");
    	docIdWithFieldValue1.put(3, "Pramod");
    	
    	Map<Integer, String> docIdWithFieldValue2 = new HashMap<Integer, String>();
    	docIdWithFieldValue2.put(1, "Karan");
    	docIdWithFieldValue2.put(2, "Singh");
    	docIdWithFieldValue2.put(3, "Rao");
    	
    	
		Map<String, Integer> dtypes = new HashMap<String, Integer>();
		dtypes.put("emp", 1);
		indexer.addDoumentTypes(dtypes);
		
		Map<String, Integer> ftypes = new HashMap<String, Integer>();
		ftypes.put("fname", 1);
		ftypes.put("lname", 2);
		indexer.addFieldTypes(ftypes);

    	indexer.addToIndex(new StandardAnalyzer(Version.LUCENE_36), "emp", "fname", docIdWithFieldValue1);
    	indexer.addToIndex(new StandardAnalyzer(Version.LUCENE_36), "emp", "lname", docIdWithFieldValue2);
    	
    	byte[] ser = indexer.toBytes();
    	
    	BitSetOrSet ids = indexer.search(ser, new StandardAnalyzer(Version.LUCENE_36), "emp", "lname", "singh");
    	for (Object a : ids.getDocumentIds()) {
			System.out.println(a.toString());
		}
    	
    	BitSetOrSet ids2 = indexer.search(ser, new StandardAnalyzer(Version.LUCENE_36), "emp", "fname", "Subhendu");
    	for (Object a : ids2.getDocumentIds()) {
			System.out.println(a.toString());
		}    	
	}
    
}
