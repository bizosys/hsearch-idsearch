package com.bizosys.unstructured;


import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.hbase.HWriter;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.idsearch.storage.DirectoryHSearch;
import com.bizosys.hsearch.idsearch.storage.donotmodify.HBaseTableSchema;
import com.bizosys.hsearch.idsearch.storage.donotmodify.HSearchTableDocuments;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;
import com.bizosys.hsearch.treetable.storage.HSearchTableReader;


public class IndexWriter {
	private DirectoryHSearch directory = new DirectoryHSearch();
	private Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_35);
	private static String unknownDocumentType = "-";
	
	private FieldTypeCodes fieldTypeCodes = null;
	private DocumentTypeCodes docTypeCodes = null;
	
	static {
		try {
			HBaseTableSchema.getInstance();
		} catch (IOException ex) {
			ex.printStackTrace(System.err);
			System.exit(1);
		}
	}
			
	public IndexWriter() throws InstantiationException {
		fieldTypeCodes = SearchConfiguration.getInstance().getFieldTypeCodes();
		docTypeCodes= SearchConfiguration.getInstance().getDocumentTypeCodes();
	}
	
	public byte[] toBytes() throws IOException {
		return directory.toBytes();
	}
	
	public void addDocument(int docId, Document doc) throws IOException {
		addDocument(docId, doc, unknownDocumentType);
	}

	public void addDocument(int docId, Document doc, String documentType) throws IOException {
		addDocument(docId, doc, documentType, analyzer);
	}

	public void addDocument(int docId, Document doc, String documentType, Analyzer analyzer) throws CorruptIndexException, IOException {
    	
		Map<String, IndexRow> uniqueRows = new HashMap<String, IndexWriter.IndexRow>();
		
		int docType = docTypeCodes.getCode(documentType);
		
		for (Fieldable field : doc.getFields() ) {
    		if ( ! field.isIndexed() ) continue;

    		uniqueRows.clear();

    		int fieldType = fieldTypeCodes.getCode(field.name());
    		
    		if ( field.isTokenized() ) {
    		
    			InputStream ba = new ByteArrayInputStream( field.stringValue().getBytes());
    			InputStreamReader reader = new InputStreamReader(ba);
        		
        		TokenStream stream = analyzer.tokenStream( field.name(), reader);
        		tokenize(stream, docId, docType , fieldType, field.getBoost(), directory, uniqueRows);
        		reader.close();

    		} else {
    			
        		String token = field.stringValue();
        		float finalBoost = field.getBoost();
        		directory.put(docType, fieldType, token.hashCode(), token, docId, finalBoost);
    		}
		}
		if ( null != uniqueRows)  uniqueRows.clear();
	}	


	public byte[] commit(String mergeId, String field) throws IOException {
		String tableName = HBaseTableSchemaDefn.getInstance().tableName;
		byte[] colNameBytes = HBaseTableSchemaDefn.getInstance().COL_NAME_BYTES;
		byte[] data = directory.toBytes();
		
		RecordScalar mergedTable = new RecordScalar(
				mergeId.getBytes(), 
				new NV(field.getBytes(), colNameBytes , data ) ) ;
		
		List<RecordScalar> records = new ArrayList<RecordScalar>(1);
    	records.add(mergedTable);
    	HWriter.getInstance(true).insertScalar(tableName, records);
    	
    	return data;
	}

	public void close() throws IOException {
		if ( null != directory) directory.clear();
		if ( null != analyzer) analyzer.close();
	}
	
	private final void tokenize(TokenStream stream, int docId, int docType, int fieldType, float fieldBoost,
		DirectoryHSearch codecs, Map<String, IndexRow> uniqueRows ) throws IOException {
		
		String token = null;
		CharTermAttribute termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
		OffsetAttribute offsetA = (OffsetAttribute) stream.getAttribute(OffsetAttribute.class);
		int lastBoost = 0;
		int curoffset = 0;

		while ( stream.incrementToken()) {
			
			token = termA.toString();
			curoffset = offsetA.endOffset();
			if ( lastBoost < curoffset) lastBoost = curoffset;
			
			IndexRow row = new IndexRow(docId, token,docType, fieldType, curoffset, fieldBoost);
			String key = row.toString();
			
			if (uniqueRows.containsKey(key) ) {
				IndexRow existingRow = uniqueRows.get(key);
				existingRow.occurance++;
			} else {
				uniqueRows.put(key, row);
			}
		}
		
		for (IndexRow row : uniqueRows.values()) {
			double occuranceBoost = Math.log10(row.occurance);
			if ( occuranceBoost > 1 ) occuranceBoost = 1;
			double locationBoost = ( lastBoost - row.offset)/ lastBoost;
			float finalBoost = (float) ( row.boost + occuranceBoost + locationBoost );
			
			codecs.put(row.docType, row.fieldType, row.token.hashCode(), row.token, row.docId, finalBoost);
		}
		stream.close();
	}	
	
	private static class IndexRow {
		
		public int docId;
		public String token;
		public int docType;
		public int fieldType;
		public float offset = 0F;
		public int occurance = 1;
		public float boost= 1F;
		
		public IndexRow(int docId, String token, int docType, int fieldType,float offset, float boost)  {
			this.docId = docId;
			this.token = token;
			this.docType = docType;
			this.fieldType = fieldType;
			this.offset = offset;
			this.boost = boost;
		}

		public String toString() {
			String uniqueKey = token + "|" +  docType + "|" + fieldType + "|" + docId;
			return uniqueKey;
		}
	}
}
