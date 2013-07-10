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
package com.bizosys.unstructured;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.util.Hashing;
import com.bizosys.unstructured.util.Constants;

public class IndexSearcher {
	SearchConfiguration sConf = null;
	public IndexSearcher() throws InstantiationException {
		this.sConf = SearchConfiguration.getInstance();
	}
	
	@Deprecated
	public String searchQueryPartsFill( String indexName, String docType, 
		String query, Analyzer analyzer, Map<String, String> multiQueryParts) throws Exception {
		
		System.err.println("\n\n\n************ Stop using this method and instead use the following method. ******************\n" + 
		"public String searchQueryPartsFill( Analyzer analyzer, boolean isAllWords, String multiQuery, Map<String, String> multiQueryParts, String... partsToAnalyze) throws Exception\n\n\n");
		
		String defaultField = "BIZOSYSNONE";
		
		QueryParser qp = new QueryParser(Version.LUCENE_36, defaultField, analyzer); 
		Query q = qp.parse(query);
		Set<Term> terms = new HashSet<Term>();
		q.extractTerms(terms);
		
		int index = 0;
		Map<String, String> termsL = new HashMap<String, String>();
		if ( ! "*".equals(docType) ) docType = this.sConf.getDocumentTypeCodes().getCode(docType).toString();
		
		for (Term term : terms) {
			String fieldName = term.field();
			if ( defaultField.equals(fieldName)) fieldName = "*";
			else if ( "*".equals(fieldName)) fieldName = "*";
			else fieldName = this.sConf.getFieldTypeCodes().getCode(term.field()).toString(); 
			
			String fieldText = term.text(); 
			
			String expandedTerm = docType + "|" + fieldName + "|" + Hashing.hash(fieldText) + "|*|*";
			
			String lhs = indexName + ":" + index;
			multiQueryParts.put( lhs , expandedTerm);
			
			String fld = term.field();
			if ( defaultField.equals(fld)) termsL.put(fieldText, lhs);
			else termsL.put(term.field() + ":" + fieldText, lhs);
			index++;
		}
		
		//Replace the intermediate ones
		for (String term : termsL.keySet()) {
			
			String caseQuery = null;
			for ( int i=0; i<3; i++) {
				switch (i) {
					case 0:
						caseQuery = query;
						break;
					case 1:
						caseQuery = query.toLowerCase();
						break;
					case 2:
						caseQuery = query.toUpperCase();
						break;
				}
				term = term.replace(defaultField + ":", "");
				int caseTermIndex = caseQuery.indexOf(term + " ") ;
				if ( caseTermIndex >= 0 ) {
					query = query.substring(0, caseTermIndex) + termsL.get(term) + query.substring(caseTermIndex + term.length());
				}
			}
		}
		
		//Replace the last one
		for (String term : termsL.keySet()) {
			String caseQuery = null;
			for ( int j=0; j<3; j++) {
				switch (j) {
					case 0:
						caseQuery = query;
						break;
					case 1:
						caseQuery = query.toLowerCase();
						break;
					case 2:
						caseQuery = query.toUpperCase();
						break;
				}
				int caseTermIndex = caseQuery.indexOf(term) ;
				if ( caseTermIndex >= 0 ) {
					query = query.substring(0, caseTermIndex) + termsL.get(term) + query.substring(caseTermIndex + term.length());
					break;
				}
			}
		}
		
		return query;
	}
	
	@Deprecated
	public String searchQueryPartsFill(String indexName, String docType, 
		String query, Map<String, String> multiQueryParts) throws Exception {
		
		return searchQueryPartsFill(indexName, docType, query, 
			new StandardAnalyzer(Constants.LUCENE_VERSION), multiQueryParts );
	}	
	
	
	public String searchQueryPartsFill( Analyzer analyzer, boolean isAllWords, String multiQuery,
		Map<String, String> multiQueryParts, String... partsToAnalyze) throws Exception {
			
			String defaultField = "BIZOSYSNONE";
			Map<Integer, String> explodedParts = new HashMap<Integer, String>();
			
			for (String qKey : partsToAnalyze) {
				QueryParser qp = new QueryParser(Version.LUCENE_36, defaultField, analyzer); 
				Set<Term> terms = new HashSet<Term>();
				Query q = qp.parse(multiQueryParts.get(qKey));
				q.extractTerms(terms);

				int index = 1;
				explodedParts.clear();
				
				for (Term term : terms) {
					String fieldName = term.field();
					String fieldText = term.text();
					String docType = "*";
					String fieldType = "*";
					int docAndFieldBreakPointIndex = fieldName.indexOf('/');
					
					if ( -1 == docAndFieldBreakPointIndex) {
						docType = fieldName;
					} else {
						docType = fieldName.substring(0, docAndFieldBreakPointIndex);
						fieldType = fieldName.substring(docAndFieldBreakPointIndex + 1);
					}

					if ( docType.equals(defaultField) ) docType = "*";
					else if ( ! ( "*".equals(docType) || "".equals(docType) ) ) {
						docType = sConf.getDocumentTypeCodes().getCode(docType).toString();
					}
					
					if ( fieldType.equals(defaultField) ) fieldType = "*";
					else if ( ! ( "*".equals(fieldType) || "".equals(fieldType) ) ) {
						fieldType = sConf.getFieldTypeCodes().getCode(fieldType).toString();
					} 

					String expandedTerm = docType + "|" + fieldType + "|" + Hashing.hash(fieldText) + "|*|*";
					explodedParts.put(index, expandedTerm);
					index++;
				}
				
				if ( explodedParts.size() > 1) {
					multiQueryParts.remove(qKey);
					
					StringBuilder sb = new StringBuilder();
					boolean isFirst = true;
					for (Integer seq : explodedParts.keySet()) {
						String explodedKey = qKey + seq.toString(); 
						multiQueryParts.put(explodedKey, explodedParts.get(seq));
						if ( isFirst ) isFirst = false;
						else {
							if ( isAllWords ) sb.append(" AND ");
							else sb.append(" OR ");
						}
						sb.append(explodedKey);
					}
					multiQuery = multiQuery.replace(qKey, " ( " + sb.toString() + " ) ");
				} else {
					multiQueryParts.put(qKey, explodedParts.get(index - 1));
				}
			}
			
			return multiQuery;
		}	
	
	public String searchQueryPartsFillWithMetadata( Analyzer analyzer, boolean isAllWords, String multiQuery,
			Map<String, String> multiQueryParts, String... partsToAnalyze) throws Exception {
				
				String defaultField = "BIZOSYSNONE";
				Map<Integer, String> explodedParts = new HashMap<Integer, String>();
				
				for (String qKey : partsToAnalyze) {
					QueryParser qp = new QueryParser(Version.LUCENE_36, defaultField, analyzer); 
					Set<Term> terms = new HashSet<Term>();
					Query q = qp.parse(multiQueryParts.get(qKey));
					q.extractTerms(terms);

					int index = 1;
					explodedParts.clear();
					
					for (Term term : terms) {
						String fieldName = term.field();
						String searchword = term.text();
						String docType = "*";
						String fieldType = "*";
						String payload = "*";
						
						int docAndFieldBreakPointIndex = fieldName.indexOf('/');
						
						if ( -1 == docAndFieldBreakPointIndex) {
							docType = fieldName;
						} else {
							docType = fieldName.substring(0, docAndFieldBreakPointIndex);
							fieldType = fieldName.substring(docAndFieldBreakPointIndex + 1);
							
							int fieldAndPayloadBreakPointIndex = fieldType.indexOf('/');
							if ( fieldAndPayloadBreakPointIndex > 0) {
								fieldType = fieldType.substring(0, fieldAndPayloadBreakPointIndex);
								payload = fieldType.substring(fieldAndPayloadBreakPointIndex + 1);
							}
						}

						if ( docType.equals(defaultField) ) docType = "*";
						else if ( ! ( "*".equals(docType) || "".equals(docType) ) ) {
							docType = sConf.getDocumentTypeCodes().getCode(docType).toString();
						}
						
						if ( fieldType.equals(defaultField) ) fieldType = "*";
						else if ( ! ( "*".equals(fieldType) || "".equals(fieldType) ) ) {
							fieldType = sConf.getFieldTypeCodes().getCode(fieldType).toString();
						} 

						String expandedTerm = docType + "|" + fieldType + "|" + payload + "|" + Hashing.hash(searchword) + "|*|*";
						explodedParts.put(index, expandedTerm);
						index++;
					}
					
					if ( explodedParts.size() > 1) {
						multiQueryParts.remove(qKey);
						
						StringBuilder sb = new StringBuilder();
						boolean isFirst = true;
						for (Integer seq : explodedParts.keySet()) {
							String explodedKey = qKey + seq.toString(); 
							multiQueryParts.put(explodedKey, explodedParts.get(seq));
							if ( isFirst ) isFirst = false;
							else {
								if ( isAllWords ) sb.append(" AND ");
								else sb.append(" OR ");
							}
							sb.append(explodedKey);
						}
						multiQuery = multiQuery.replace(qKey, " ( " + sb.toString() + " ) ");
					} else {
						multiQueryParts.put(qKey, explodedParts.get(index - 1));
					}
				}
				
				return multiQuery;
			}		
}
