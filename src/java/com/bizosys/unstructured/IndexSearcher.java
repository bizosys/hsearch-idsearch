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
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.idsearch.storage.Client;
import com.bizosys.hsearch.idsearch.storage.DirectoryHSearch;
import com.bizosys.hsearch.idsearch.storage.ResultToStdout;

public class IndexSearcher {
	public DirectoryHSearch directory = new DirectoryHSearch();
	private FieldTypeCodes fieldTypeCodes = null;

	public IndexSearcher() throws InstantiationException {
		fieldTypeCodes = SearchConfiguration.getInstance().getFieldTypeCodes();
	}
	
	public ScoreDoc[] search(String defaultField, String query, Analyzer analyzer) throws Exception {
		long s = System.currentTimeMillis();
		Query q = new QueryParser(Version.LUCENE_35, defaultField, analyzer).parse(query);
		Set<Term> terms = new HashSet<Term>();
		q.extractTerms(terms);
		
		Map<String, String> multiQueryParts = new HashMap<String, String>();
		int index = 0;
		Map<String, String> termsL = new HashMap<String, String>();
		
		String colName = "Documents" ;
		
		for (Term term : terms) {
			Integer fieldName = fieldTypeCodes.getCode(term.field()); 
			String fieldText = term.text(); 
			
			String expandedTerm = "*|" + fieldName.toString() + "|*|" + fieldText + "|*|*";
			String lhs = colName + ":" + index;
			multiQueryParts.put( lhs , expandedTerm);
			termsL.put(fieldName.toString() + ":" + fieldText, lhs);
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
		
		long e = System.currentTimeMillis();
		
		System.out.println(query + "\n" + multiQueryParts.toString() + ".\nTime Taken in (ms) " + ( e - s));
		
		Client client = new Client(new ResultToStdout());
		client.execute("Documents:0 AND Documents:1", multiQueryParts);
		return null;
	}
	
	public ScoreDoc[] search(String field, String query) throws Exception {
		//directory.get(input, new HSearchQuery("*|" + query + "|*|*|*|*"), mapper);
		return null;
	}	
	
	public static void main(String[] args) throws Exception {
		new IndexSearcher().search(
			"f", "ABINASH", new StandardAnalyzer(Version.LUCENE_35));
	}
}
