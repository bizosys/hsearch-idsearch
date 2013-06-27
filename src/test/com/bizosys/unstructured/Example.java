package com.bizosys.unstructured;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;

import com.bizosys.hsearch.storage.Client;
import com.bizosys.hsearch.storage.donotmodify.HBaseTableSchema;
import com.bizosys.hsearch.storage.donotmodify.HSearchTableDocuments;

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

		doc.add(new Field("id", "DOC 001", Field.Store.NO, Field.Index.NOT_ANALYZED));
		doc.add(new Field("name", "Abinash", Field.Store.NO, Field.Index.NOT_ANALYZED));
		doc.add(new Field("city", "Abinash Bangalore", Field.Store.NO, Field.Index.ANALYZED));
		doc.add(new Field("description", "Abinash works in Big Data. Abinash also plays badminton", Field.Store.NO, Field.Index.ANALYZED));
		 
		String INDEX_NAME = "Documents";

		Analyzer analyzer = getAnalyzer();
		AnalyzerFactory analyzers = new AnalyzerFactory(analyzer);

		IndexWriter writer = new IndexWriter(new HSearchTableDocuments());
		 try {
			 writer.addDocument(1, doc, "emp", analyzers);
    	} finally {
    		analyzers.close();
    	}

		HBaseTableSchema.getInstance().getSchema(); 
		//writer.commit("merge1", INDEX_NAME);
		if ( null != writer) writer.close();
		
		Analyzer qAnalyzer = getAnalyzer();
		Map<String, String> multiqueryParts = new HashMap<String, String>();
		String multiQuery = new IndexSearcher().searchQueryPartsFill(
			INDEX_NAME, "*", "*:Abinash", qAnalyzer, multiqueryParts);
		if ( null != qAnalyzer ) qAnalyzer.close();
		
	  	Client ht = new Client();
        ht.execute("demo-table", multiQuery, multiqueryParts);
        
	}

	public static Analyzer getAnalyzer() {
		Analyzer analyzer = new CustomAnalyzer();
		return analyzer;
	}
}
