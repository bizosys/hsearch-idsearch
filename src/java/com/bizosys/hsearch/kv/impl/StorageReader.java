package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.byteutils.SortedBytesBitsetCompressed;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.kv.dao.KVRowReader;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.Hashing;

public class StorageReader implements Callable<Map<Integer, Object>> {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	public KVDocIndexer indexer = new KVDocIndexer();
	
	public String tableName;
	public String rowId;
	public BitSetOrSet matchingIds;
	public String filterQuery;
	public HSearchProcessingInstruction instruction = null;
	public Analyzer analyzer = null;
	
	public StorageReader(final String tableName, final String rowId, final BitSetOrSet matchingIds, final String filterQuery, 
						 final HSearchProcessingInstruction instruction, final Analyzer analyzer) {
		this.tableName = tableName;
		this.rowId = rowId;
		this.matchingIds = matchingIds;
		this.filterQuery = filterQuery;
		this.instruction = instruction;
		this.analyzer = analyzer;
	}

	@Override
	public Map<Integer, Object> call() throws Exception {
		return readStorageValues();
	}
	
	public final Map<Integer, Object> readStorageValues() throws IOException {

		byte[] data = null;
		Map<Integer, Object> finalResult = new HashMap<Integer, Object>(); 

		try {
			ComputeKV compute = new ComputeKV();
			compute.kvType = (instruction.getOutputType() == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : instruction.getOutputType();
			compute.kvRepeatation = instruction.getProcessingHint().startsWith("true");
			compute.isCompressed = instruction.getProcessingHint().endsWith("true");
			compute.rowContainer = finalResult;
			
			long start = System.currentTimeMillis();
			if(null == matchingIds){
				finalResult = KVRowReader.getAllValues(tableName, rowId.getBytes(), 
					compute, this.filterQuery, instruction);
			} else {
				List<BitSet> ids = new ArrayList<BitSet>(1);
				ids.add(matchingIds.getDocumentSequences());
				byte[] matchingIdsB = SortedBytesBitset.getInstance().toBytes(ids); 
				data = KVRowReader.getFilteredValues(tableName, rowId.getBytes(), matchingIdsB, filterQuery, instruction);
				compute.put(data);						
			}
			
			if(DEBUG_ENABLED){
				long end = System.currentTimeMillis();
				if(null != data)
					HSearchLog.l.debug(rowId + " Fetch time " + (end - start) +" for " + data.length +" bytes");
				else
					HSearchLog.l.debug(rowId + " Fetch time " + (end - start) +" for " + finalResult.size() +" records");
			}

		} catch (Exception e) {
			String msg = e.getMessage() + "\nFor rowid = "+ rowId + " query = " + filterQuery;
			HSearchLog.l.fatal("ReadStorage Exception " + msg , e );
			e.printStackTrace();
			throw new IOException(msg, e);
		}

		
		return finalResult;

	}

	public final BitSet readStorageIds() throws IOException {
		byte[] data = null;
		try {
			data = KVRowReader.getFilteredValues(tableName, rowId.getBytes(), null, filterQuery, instruction);
			
			Collection<byte[]> dataL = SortedBytesArray.getInstanceArr().parse(data).values();
			
			byte[] dataChunk = dataL.isEmpty() ? null : dataL.iterator().next();
			List<BitSet> ids = new ArrayList<BitSet>();
			SortedBytesBitset.getInstance().parse(dataChunk).values(ids);
			return (ids.size() == 0 ) ? new BitSet() : ids.get(0);
			
		} catch (Exception e) {
			IOException ioException = new IOException(new String(new String(data)), e);
			throw ioException;
		}
	}

	public final BitSet readStorageTextIds(final String fieldName) throws IOException{

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

		byte[] dataChunk = null;
		try {

			Map<String, Integer> dTypes = new HashMap<String, Integer>(1);
			dTypes.put(docType, 1);
			setDocumentTypeCodes(dTypes);
			
			Map<String, Integer> fTypes= new HashMap<String, Integer>(1);
			fTypes.put(fieldType, 1);
			setFieldTypeCodes(fTypes);
			
			int queryPartLoc = filterQuery.lastIndexOf('|');
			String query = ( queryPartLoc < 0 ) ? filterQuery : filterQuery.substring(queryPartLoc + 1);
			QueryParser qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
			Query q = null;
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
				filterQuery = sb.append(docTypeCode).append('|').append(fldTypeCode).append('|')
				           .append('*').append('|').append(hash).append('|').append("*|*").toString();
				sb.delete(0, sb.length());
				currentRowId = mergeid + "_" + wordHash.charAt(0) + "_" + wordHash.charAt(wordHash.length() - 1);
				
				byte[] data = KVRowReader.getFilteredValues(tableName, currentRowId.getBytes(), null, filterQuery, instruction);
				
				Collection<byte[]> dataL = SortedBytesArray.getInstanceArr().parse(data).values();
				dataChunk = dataL.isEmpty() ? null : dataL.iterator().next();
				List<BitSet> ids = new ArrayList<BitSet>();
				System.out.println("################");
				SortedBytesBitset.getInstance().parse(dataChunk).values(ids);
				BitSet bitSets = (ids.size() == 0 ) ? new BitSet() : ids.get(0);
				
				if(isFirst){
					destination.setDocumentSequences(bitSets);
					isFirst = false;
					continue;					
				}
				else{
					BitSetOrSet source = new BitSetOrSet();
					source.setDocumentSequences(bitSets);
					destination.or(source);
				}
			}
			
			return destination.getDocumentSequences();
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(new String(dataChunk), e);
		} 
	}

	
	public final void setFieldTypeCodes(final Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public final void setDocumentTypeCodes(final Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}	
}


