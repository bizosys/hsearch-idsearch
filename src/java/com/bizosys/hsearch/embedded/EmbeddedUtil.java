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

public class EmbeddedUtil {
	
	IndexWriter writer = null;

	public void addToIndex(Analyzer analyzer, String docType, String fieldType, Map<Integer, String> docIdWithFieldValue) throws IOException, InstantiationException {
		if ( null == writer) writer = new IndexWriter(new HSearchTableDocuments());
    	AnalyzerFactory analyzers = new AnalyzerFactory(analyzer);
    	
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
    	
    	int docTypeCode = DocumentTypeCodes.getInstance().getCode(docType);
    	int fldTypeCode = FieldTypeCodes.getInstance().getCode(fieldType);

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
			System.out.println(fieldText);
			if ( null == allWords) {
				allWords = new StringBuilder("{");
				allWords.append(Hashing.hash(fieldText));
			} else {
				allWords.append(',').append(Hashing.hash(fieldText));
			}
		}
		allWords.append('}');
		System.out.println(allWords.toString());
		
    	HSearchQuery hq = new HSearchQuery(docTypeCode + "|" + fldTypeCode + "|" + allWords.toString() + "|*|*");
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

    public static void main(String[] args) throws IOException, InstantiationException, ParseException {

		EmbeddedUtil indexer = new EmbeddedUtil();
    	
    	Map<Integer, String> docIdWithFieldValue = new HashMap<Integer, String>();
    	docIdWithFieldValue.put(1, "Abinash Karan");
    	docIdWithFieldValue.put(2, "Subhendu Singh");
    	docIdWithFieldValue.put(3, "Pramod Rao");
    	
		Map<String, Integer> dtypes = new HashMap<String, Integer>();
		dtypes.put("emp", 1);
		indexer.addDoumentTypes(dtypes);
		
		Map<String, Integer> ftypes = new HashMap<String, Integer>();
		ftypes.put("name", 1);
		indexer.addFieldTypes(ftypes);

    	indexer.addToIndex(new StandardAnalyzer(Version.LUCENE_36), "emp", "name", docIdWithFieldValue);
    	byte[] ser = indexer.toBytes();
    	System.out.println(ser.length);
    	
    	System.out.println( indexer.search(ser, new StandardAnalyzer(Version.LUCENE_36), "emp", "name", "Abinash Pramod").toString() );
    	
	}
    
}
