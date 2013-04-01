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

import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.unstructured.util.Constants;

public class IndexSearcher {
	private DocumentTypeCodes docTypeCodes = null;
	private FieldTypeCodes fieldTypeCodes = null;

	public IndexSearcher() throws InstantiationException {
		SearchConfiguration sConf = SearchConfiguration.getInstance();
		fieldTypeCodes = sConf.getFieldTypeCodes();
		docTypeCodes  = sConf.getDocumentTypeCodes();
	}
	
	public String searchQueryPartsFill( String indexName, String docType, 
		String query, Analyzer analyzer, Map<String, String> multiQueryParts) throws Exception {
		
		String defaultField = "BIZOSYSNONE";
		
		QueryParser qp = new QueryParser(Version.LUCENE_36, defaultField, analyzer); 
		Query q = qp.parse(query);
		Set<Term> terms = new HashSet<Term>();
		q.extractTerms(terms);
		
		int index = 0;
		Map<String, String> termsL = new HashMap<String, String>();
		if ( ! "*".equals(docType) ) docType = docTypeCodes.getCode(docType).toString();
		
		for (Term term : terms) {
			
			String fieldName = term.field();
			if ( defaultField.equals(fieldName)) fieldName = "*";
			else if ( "*".equals(fieldName)) fieldName = "*";
			else fieldName = fieldTypeCodes.getCode(term.field()).toString(); 
			
			String fieldText = term.text(); 
			System.out.println(fieldText);
			
			String expandedTerm = 
				docType + "|" + fieldName + "|" + Hashing.hash(fieldText) + "|*|*";
			
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
				//System.out.println( "**** " + caseQuery + "\t\t-\t\t" + term);
				term = term.replace(defaultField + ":", "");
				int caseTermIndex = caseQuery.indexOf(term + " ") ;
				//System.out.println( "#### " + term + "\t\t-\t\t" + caseTermIndex);
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
		System.out.println(query + "\n" + multiQueryParts.toString());
		
		return query;
	}
	
	public String searchQueryPartsFill(String indexName, String docType, 
		String query, Map<String, String> multiQueryParts) throws Exception {
		
		return searchQueryPartsFill(indexName, docType, query, 
			new StandardAnalyzer(Constants.LUCENE_VERSION), multiQueryParts );
	}	
	
	public static void main(String[] args) throws Exception {
		Map<String, Integer> fldTypes = new HashMap<String, Integer>();
		fldTypes.put("name", 2);
		SearchConfiguration.getInstance().instantiateFieldTypeCodes(fldTypes);
		
		Map<String, Integer> docTypes = new HashMap<String, Integer>();
		docTypes.put("emp", 1);
		SearchConfiguration.getInstance().instantiateDocumentTypeCodes(docTypes);

		Map<String, String> multiquery = new HashMap<String, String>();
		String query = new IndexSearcher().searchQueryPartsFill(
			"Documents", "*", "(abinash AND name:ava) OR name:jyoti", multiquery);
		System.out.println(query + "\n" + multiquery.toString());
		
		System.out.println( "abinash" + "\t" + Hashing.hash("abinash"));
		System.out.println( "ava" + "\t" + Hashing.hash("ava"));
		System.out.println( "jyoti" + "\t" + Hashing.hash("jyoti"));
	}
}
