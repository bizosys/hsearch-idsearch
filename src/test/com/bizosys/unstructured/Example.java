package com.bizosys.unstructured;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.idsearch.storage.Client;
import com.bizosys.hsearch.idsearch.storage.donotmodify.HSearchTableDocuments;
import com.bizosys.hsearch.treetable.client.HSearchQuery;

public class Example {
	
	public static void main(String[] args) throws Exception  {
		
		SearchConfiguration conf = SearchConfiguration.getInstance();
		Map<String, Integer> docTypes = new HashMap<String, Integer>();
		docTypes.put("emp", 1);
		conf.instantiateDocumentTypeCodes(docTypes);
		

		Map<String, Integer> fldTypes = new HashMap<String, Integer>();
		fldTypes.put("id", 1);
		fldTypes.put("name", 2);
		fldTypes.put("city", 3);
		fldTypes.put("description",4);
		conf.instantiateFieldTypeCodes(fldTypes);
		
		Document doc = new Document();
		 doc.add(new Field("id", "DOC001", Field.Store.YES, Field.Index.NO));
		 doc.add(new Field("name", "Abinash" , Field.Store.YES, Field.Index.ANALYZED));
		 doc.add(new Field("city", "Abinash Bangalore", Field.Store.YES, Field.Index.NOT_ANALYZED));
		 doc.add(new Field("description", "Abinash Abinash He works in Big Data", Field.Store.YES, Field.Index.ANALYZED));	
		 
		 Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
		 IndexWriter writer = new IndexWriter();
		 try {
			 writer.addDocument(1, doc, "emp", analyzer);
    	} finally {
    		if ( null != analyzer ) analyzer.close();
    	}
		//writer.commit("merge1", "Documents");
		if ( null != writer) writer.close();
		
		Analyzer qAnalyzer = new StandardAnalyzer(Version.LUCENE_35);
		Map<String, Float> outputIds = new IndexSearcher().search("Documents", "name:Abinash AND description:Big", qAnalyzer);
		if ( null != qAnalyzer ) qAnalyzer.close();
		
		for ( String docId :  outputIds.keySet()) {
			System.out.println("**** Found Document\t\t" + docId.toString() + "-" + outputIds.get(docId));
		}
		
		/**
		Map<String, Float> output = new HashMap<String, Float>();
	  	Client ht = new Client(output);
        Map<String, String> multiQueryParts = new HashMap<String, String>();
        int hashCode = "abinash".hashCode();
        multiQueryParts.put("Documents:1", "*|*|*|*|*|*");
        
        long start = System.currentTimeMillis();
        ht.execute("Documents:1", multiQueryParts);
        long end = System.currentTimeMillis();
        System.out.println(" finished in  " + (end - start) + " millis ");	
        */			
	}
}
