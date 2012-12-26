package com.bizosys.hsearch.unstructured.tokenizer;

import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.idsearch.table.TermTable;
import com.bizosys.hsearch.unstructured.util.ContentFieldReader;
import com.bizosys.hsearch.unstructured.util.Document;
import com.bizosys.hsearch.unstructured.util.IndexObjectFactory;
import com.bizosys.hsearch.unstructured.util.TermStream;

public class PipeInData {

	/**
	 * Supplied Data
	 */
	public List<Document> suppliedInputDocs = new ArrayList<Document>();
	
	
	/**
	 * Processing document intermediate calculations
	 */
	public Document processingDoc = null;
	public  List<TermStream> processingDocTokenStreams = IndexObjectFactory.getInstance().getStreamList();
	public List<ContentFieldReader> processingDocFieldReaders = null;
	
	/**
	 * Final Output
	 */
	public TermTable termsFromAllDocuments;
	
}
