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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.bizosys.hsearch.embedded.donotmodify.HSearchTableDocuments;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.FederatedSearch;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.unstructured.AnalyzerFactory;
import com.bizosys.unstructured.IndexSearcher;
import com.bizosys.unstructured.IndexWriter;

public class EmbeddedHSearch {
	
	private static boolean DEBUG_MODE = EmbeddedHSearchLog.l.isDebugEnabled();
	@SuppressWarnings("unused")
	private static boolean INFO_MODE = EmbeddedHSearchLog.l.isInfoEnabled();
	
    IndexWriter writer = null;
    byte[] indexBytes = null;
    boolean isAllWords = false;
    
	Analyzer analyzer = null;
	FederatedSearch ff = createFederatedSearch(); 
    
    public EmbeddedHSearch(Set<String> stopwords, Map<String, String> synonums, char synonumSeparator) throws InstantiationException {
    	this.writer = new IndexWriter(new HSearchTableDocuments());
    	analyzer = new CustomAnalyzer(stopwords, synonums);
    }
    
    public EmbeddedHSearch(CustomAnalyzer analyzer) throws InstantiationException {
    	this.writer = new IndexWriter(new HSearchTableDocuments());
    	this.analyzer = analyzer;
    }

    public Analyzer getAnalyzer() {
    	return analyzer;
    }

    public void setAnalyzer(Analyzer analyzer) throws IOException {
    	AnalyzerFactory.getInstance().setDefault(analyzer);
    }

    public void addToIndex(String docType, String fieldType, Map<Integer, String> docIdWithFieldValue) throws IOException, InstantiationException {
    	
		for (Integer docId : docIdWithFieldValue.keySet()) {
		    Document lucenDoc = new Document();
		    lucenDoc.add(new Field(fieldType, docIdWithFieldValue.get(docId), Field.Store.YES, Field.Index.ANALYZED));
		    writer.addDocument(docId, lucenDoc,docType, AnalyzerFactory.getInstance());
		}
	}
    
    public void commit() throws IOException {
		this.indexBytes = writer.toBytes();
    }
    
	private FederatedSearch createFederatedSearch() {
		
		FederatedSearch ff = new FederatedSearch() {
			@Override
			public BitSetOrSet populate(
					String type, String queryId, String queryDetail, Map<String, Object> params) throws IOException{
				
				try {
					MapperDocuments md  = new MapperDocuments();
					HSearchQuery hq = new HSearchQuery(queryDetail);
					HSearchTableDocuments htd = new HSearchTableDocuments();
					htd.get((byte[])params.get("data"), hq, md);
					return md.getUniqueMatchingDocumentIds();
				} catch (Exception ex) {
					throw new IOException(ex);
				}
			}
		};
		return ff;
	}    
	
	public void setTofindAllWords(boolean isAllWords) {
		this.isAllWords = isAllWords;
	}
	
	@SuppressWarnings("unchecked")
	public Set<Integer> searchIds(String multiQuery, Map<String, String> multiqueryParts, String... textPart) throws InstantiationException, Exception {

		multiQuery = new IndexSearcher().searchQueryPartsFill(
        		this.analyzer, this.isAllWords, multiQuery, multiqueryParts, textPart);
		
		if ( DEBUG_MODE ) {
			EmbeddedHSearchLog.l.debug("\n" + multiQuery + "\n" + this.isAllWords + "\n" + multiqueryParts.toString());
		}

		Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
		for (String qPart : multiqueryParts.keySet()) {
			QueryPart qp = new QueryPart(multiqueryParts.get(qPart));
			qp.setParam("data", this.indexBytes);
			queryDetails.put(qPart,  qp);
		}
		
		BitSetOrSet manyResult = ff.execute(multiQuery, queryDetails);
		//System.out.println("Final Result:" + manyResult.getDocumentIds().toString());
		return manyResult.getDocumentIds();
	}
	
	public Set<Integer> searchIds(String query) throws InstantiationException, Exception {
		String multiQuery = "Documents:A"; //Documents:A AND Findings:B
		Map<String, String> multiqueryParts = new HashMap<String, String>();
		multiqueryParts.put("Documents:A", query);
		return searchIds(multiQuery,multiqueryParts,"Documents:A");
	}

	public void closeWriter() throws IOException {
		this.writer.close();
	}
	
	public void close() throws IOException {
		if ( null != this.writer ) this.writer.close();
	} 		
}

