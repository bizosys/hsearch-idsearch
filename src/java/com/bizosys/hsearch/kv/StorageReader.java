package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.KVDocIndexer;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.Hashing;

public class StorageReader implements Callable<Object> {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	public KVDataSchemaRepository repository = null;
	public String schemaRepositoryName = null;

	public KVDocIndexer indexer = null;
	public Analyzer analyzer = null;
	
	public String tableName;
	public String rowId;
	public String filter;
	public int callBackType;	
	
	public StorageReader(KVDataSchemaRepository repository,
			String schemaRepositoryName, KVDocIndexer indexer, Analyzer analyzer,
			String tableName, String rowId, String filter,int callBackType) {
		
		this.repository = repository;
		this.schemaRepositoryName = schemaRepositoryName;
		this.indexer = indexer;
		this.analyzer = analyzer;
		this.tableName = tableName;
		this.rowId = rowId;
		this.filter = filter;
		this.callBackType = callBackType;
	}

	@Override
	public Object call() throws Exception {
		Object obk = readStorage();
		return obk;
	}
	
	public final Object readStorage() {
		
		byte[] data = null;
		String fieldName = null;
		KVDataSchema dataScheme = null;
		Field fld = null;
		try {
			
			fieldName = rowId.substring(rowId.lastIndexOf('_') + 1, rowId.length());
			dataScheme = repository.get(schemaRepositoryName);
			fld = dataScheme.fm.nameSeqs.get(fieldName);
			int outputType = dataScheme.dataTypeMapping.get(fieldName);
			
			switch (callBackType) {
				case HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS:
					//if the field is not saved it canot be fetched 
					if ( !fld.isStored ) {
						HSearchLog.l.fatal("Field: " + fieldName + " is not saved cannot be selected ");
						throw new IOException("Field: " + fieldName + " is not saved cannot be selected ");
					}

					ComputeKV compute = new ComputeKV();
					compute.kvType = outputType;
					compute.rowContainer = new HashMap<Integer, Object>();
					
					long start = System.currentTimeMillis();
					data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, compute.kvType);
					compute.put(data);
					
					if(DEBUG_ENABLED){
						long end = System.currentTimeMillis();
						HSearchLog.l.debug("Hbase Fetch time " + (end - start) +" for " + data.length +" bytes");
					}

					return compute.rowContainer;
					
				case HSearchProcessingInstruction.PLUGIN_CALLBACK_ID:
					
					//call freetext if it is
					if(fld.isDocIndex && !fld.isStored){
						return freeTextSearch(fieldName, outputType);
		    		}
					else{
						data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, outputType);
						byte[] dataChunk = SortedBytesArray.getInstanceArr().parse(data).values().iterator().next();
						Set<Integer> ids = new HashSet<Integer>();
						SortedBytesInteger.getInstance().parse(dataChunk).values(ids);
						return ids;
					}
			}
		} catch (Exception e) {
			String msg = e.getMessage() + "\nField :" + fieldName + " rowid "+ rowId + " query " + filter;
			if ( null != dataScheme ) {
				if ( null != dataScheme.dataTypeMapping ) {
					msg = msg + "\tdataScheme>" + dataScheme.dataTypeMapping.toString();
				}
			}
			HSearchLog.l.fatal("ReadStorage Exception " + msg , e );
			e.printStackTrace();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public final Set<Integer> freeTextSearch(final String fieldName, final int outputType) throws IOException,ParseException, InstantiationException, java.text.ParseException, FederatedSearchException {
		
		StringBuilder sb = new StringBuilder();
		String docType = "*";
		String fieldType = fieldName;
		String wordHash = null;
		int hash = 0;
		BitSetOrSet destination  = new BitSetOrSet();
		boolean isFirst = true;
		String currentRowId = null;
		String mergeid = rowId.substring(0, rowId.lastIndexOf('_'));
		
		int fieldTypeLoc = fieldName.indexOf('/');
		if ( fieldTypeLoc > 0 ) {
			docType = fieldName.substring(0, fieldTypeLoc);
			fieldType = fieldName.substring(fieldTypeLoc + 1);
		}

		Map<String, Integer> dTypes = new HashMap<String, Integer>(1);
		dTypes.put(docType, 1);
		setDocumentTypeCodes(dTypes);
		
		Map<String, Integer> fTypes= new HashMap<String, Integer>(1);
		fTypes.put(fieldType, 1);
		setFieldTypeCodes(fTypes);
		
		int queryPartLoc = filter.lastIndexOf('|');
		String query = ( queryPartLoc < 0 ) ? filter : filter.substring(queryPartLoc + 1);
		QueryParser qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
		Query q = null;
		try {
			q = qp.parse(query);
			Set<Term> terms = new HashSet<Term>();
			q.extractTerms(terms);
			
	    	String docTypeCode = "*".equals(docType) ? "*" :
	    		new Integer(DocumentTypeCodes.getInstance().getCode(docType)).toString();
	    	
	    	String fldTypeCode = "*".equals(fieldType) ? "*" :
	    		new Integer(FieldTypeCodes.getInstance().getCode(fieldType)).toString();
	    	
			for (Term term : terms) {
				hash = Hashing.hash(term.text());
				wordHash = new Integer(hash).toString();
				sb.delete(0, sb.length());
				filter = sb.append(docTypeCode).append('|').append(fldTypeCode).append('|')
		    	           .append('*').append('|').append(hash).append('|').append("*|*").toString();
				sb.delete(0, sb.length());
				currentRowId = mergeid + "_" + wordHash.charAt(0) + "_" + wordHash.charAt(wordHash.length() - 1);
				
				byte[] data = KVRowReader.getAllValues(tableName, currentRowId.getBytes(), filter, callBackType, outputType);
				byte[] dataChunk = SortedBytesArray.getInstanceArr().parse(data).values().iterator().next();
				Set<Integer> ids = new HashSet<Integer>();
				SortedBytesInteger.getInstance().parse(dataChunk).values(ids);
				
				if(isFirst){
					destination.setDocumentIds(ids);
					isFirst = false;
					continue;					
				}
				else{
					BitSetOrSet source = new BitSetOrSet();
					source.setDocumentIds(ids);
					destination.or(source);
				}
			}
			
		} catch ( ParseException ex) {
			throw new ParseException(ex.getMessage());
		}
		return destination.getDocumentIds();
	}

	
	public final void setFieldTypeCodes(final Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public final void setDocumentTypeCodes(final Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}	
}
