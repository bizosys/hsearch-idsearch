/*
* Copyright 2013 Bizosys Technologies Limited
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
package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;
import org.iq80.snappy.Snappy;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.kv.dao.DataBlock;
import com.bizosys.hsearch.kv.dao.KvRowReaderFactory;
import com.bizosys.hsearch.kv.dao.MapperKV;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.unstructured.AnalyzerFactory;
import com.bizosys.unstructured.util.IdSearchLog;

public class StorageReader implements Callable<Map<Integer, Object>> {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	public KVDocIndexer indexer = new KVDocIndexer();
	
	public String tableName;
	public String rowId;
	public byte[] matchingIdsB;
	public String filterQuery;
	public HSearchProcessingInstruction instruction = null;
	public String fieldName = null;
	public BitSetOrSet matchingIds = null;
	
	
	boolean isCachable = true;

	public StorageReader(final String tableName, final String rowId, final String filterQuery, 
			 final HSearchProcessingInstruction instruction, boolean isCachable) {
		this.tableName = tableName;
		this.rowId = rowId;
		this.filterQuery = filterQuery;
		this.instruction = instruction;
		this.isCachable = isCachable;
	}
	
	public StorageReader(final String tableName, final String rowId, 
			final BitSetOrSet matchingIds, final byte[] matchingIdsB, 
			final String fieldName, final String filterQuery, 
			final HSearchProcessingInstruction instruction, 
			boolean isCachable) {
		
		this.tableName = tableName;
		this.rowId = rowId;
		this.matchingIds = matchingIds;
		this.matchingIdsB = matchingIdsB;
		this.fieldName = fieldName;
		this.filterQuery = filterQuery;
		this.instruction = instruction;
		this.isCachable = isCachable;
	}

	@Override
	public Map<Integer, Object> call() throws Exception {
		return readStorageValues();
	}
	
	public final Map<Integer, Object> readStorageValues() throws IOException {

		Map<Integer, Object> finalResult = null;

		try {
			ComputeKV compute = new ComputeKV();
			compute.kvType = (instruction.getOutputType() == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : instruction.getOutputType();
			compute.kvRepeatation = instruction.getProcessingHint().startsWith("true");
			compute.isCompressed = instruction.getProcessingHint().endsWith("true");
			
			long start = -1L;
			if ( DEBUG_ENABLED ) start = System.currentTimeMillis();
			
			/**
			 * Check out for pinned fields
			 */
			KvRowReaderFactory readerFactory = new KvRowReaderFactory(isCachable);
			if(null == matchingIdsB){
				finalResult = readerFactory.reader.getAllValues(tableName, rowId.getBytes(), 
					compute, this.filterQuery, instruction);
			} else {
				finalResult = readerFactory.reader.getFilteredValues(tableName, rowId.getBytes(), 
					compute, matchingIdsB, matchingIds.getDocumentSequences(), filterQuery, instruction);
			}

			if(DEBUG_ENABLED){
				long end = System.currentTimeMillis();
				if(null != finalResult)
					HSearchLog.l.debug(rowId + " Fetch time " + (end - start) +" for " + finalResult.size() + " records");
			}

		} catch (Exception e) {
			String msg = e.getMessage() + "\nFor rowid = "+ rowId + " query = " + filterQuery;
			HSearchLog.l.fatal("ReadStorage Exception " + msg , e );
			e.printStackTrace();
			throw new IOException(msg, e);
		}

		
		return finalResult;

	}

	/**
	 * Compression is taken care by the instruction hint.
	 * @return
	 * @throws IOException
	 */
	public final BitSetWrapper readStorageIds() throws IOException {
		byte[] data = null;
		try {
			
			BitSetWrapper bitSets = null;
			if ( isCachable) {
				byte[] dataA = DataBlock.getBlock(tableName, rowId, isCachable);
				int size = ( null == dataA) ? 0 : 1;
				if ( size > 0 ) {
			    	IHSearchTable table = ScalarFilter.createTable(instruction);
					HSearchQuery hq = new HSearchQuery(filterQuery);
					MapperKV map = new MapperKV();
					map.setOutputType(instruction);
					table.keySet(dataA, hq, map);
					bitSets = map.returnIds;
				}
				
			} else {
				data = DataBlock.getFilteredValuesIpc(
					tableName, rowId.getBytes(), null, filterQuery, instruction);
				Collection<byte[]> dataL = SortedBytesArray.getInstanceArr().parse(data).values();
				byte[] dataChunk = dataL.isEmpty() ? null : dataL.iterator().next();
				if ( null != dataChunk ) {
					bitSets = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataChunk, 0, dataChunk.length);
				}
			}
		
			if ( null == bitSets)  bitSets = new BitSetWrapper(0);
			return bitSets;
		
		} catch (Exception e) {
			String msg = ( null == data) ? "Null Data" :  new String(data);
			msg = e.getMessage()  + "\n" + msg ;
			IOException ioException = new IOException(msg, e);
			throw ioException;
		}
	}

	public final BitSetWrapper readStorageTextIds(Field fld,
		boolean checkForAllWords, final String fieldName) throws IOException{
		
		if ( fld.isRepeatable ) {
			return readStorageTextIdsBitset(checkForAllWords, 
				fld.biWord, fld.isCompressed, fld.isCachable, fieldName);
		} else {
			return readStorageTextIdsSet(checkForAllWords, fieldName);
		}
	}
	
	private final BitSetWrapper readStorageTextIdsSet(final boolean checkForAllWords, final String fieldName) throws IOException{

		StringBuilder sb = new StringBuilder();
		String docType = "*";
		String fieldType = fieldName;
		String wordHash = null;
		int hash = 0;
		BitSetOrSet destination  = new BitSetOrSet();
		boolean isVirgin = true;
		String currentRowId = null;

		String mergeid = rowId.substring(0, rowId.lastIndexOf('_'));
		int fieldTypeLoc = fieldName.indexOf('/');
		if ( fieldTypeLoc > 0 ) {
			docType = fieldName.substring(0, fieldTypeLoc);
			fieldType = fieldName.substring(fieldTypeLoc + 1);
		}

		byte[] dataChunk = null;
		String query = null;
		try {

			Map<String, Integer> dTypes = new HashMap<String, Integer>(1);
			dTypes.put(docType, 1);
			setDocumentTypeCodes(dTypes);
			
			Map<String, Integer> fTypes= new HashMap<String, Integer>(1);
			fTypes.put(fieldType, 1);
			setFieldTypeCodes(fTypes);
			
			int queryPartLoc = filterQuery.lastIndexOf('|');
			query = ( queryPartLoc < 0 ) ? filterQuery : filterQuery.substring(queryPartLoc + 1);
			int qLen = ( null == query) ? 0 : query.trim().length();  
			if ( 0 == qLen) {
				throw new IOException(filterQuery + " > Query is Null/Blank while processing field " + fieldName );
			}
			Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fieldName);
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
				if ( DEBUG_ENABLED) {
					IdSearchLog.l.debug("Finding Term :" + term.text());
				}
				
				hash = Hashing.hash(term.text());
				wordHash = new Integer(hash).toString();
				sb.delete(0, sb.length());
				filterQuery = sb.append(docTypeCode).append('|').append(fldTypeCode).append('|')
				           .append('*').append('|').append(hash).append('|').append("*|*").toString();
				sb.delete(0, sb.length());
				currentRowId = mergeid + "_" + wordHash.charAt(0) + "_" + wordHash.charAt(wordHash.length() - 1);
				
				byte[] data = DataBlock.getFilteredValuesIpc(
					tableName, currentRowId.getBytes(), null, filterQuery, instruction);
				
				Collection<byte[]> dataL = SortedBytesArray.getInstanceArr().parse(data).values();
				int size = ( null == dataL) ? 0 : dataL.size();

				if ( checkForAllWords ) {
					if ( size > 0 ) {
						dataChunk = dataL.isEmpty() ? null : dataL.iterator().next();
						if ( dataChunk == null ) {
							destination.clear();
							break;
						}
					} else {
						destination.clear();
						break;
					}
					
					BitSetWrapper bitSets = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataChunk, 0, dataChunk.length);
					
					if ( isVirgin ) {
						destination.setDocumentSequences(bitSets);
						isVirgin = false;
						continue;					
					}
					
					BitSetOrSet source = new BitSetOrSet();
					source.setDocumentSequences(bitSets);
					destination.and(source);
						
				} else {
					
					if ( size == 0 ) continue;
					dataChunk = dataL.isEmpty() ? null : dataL.iterator().next();
					if ( dataChunk == null ) continue;
					BitSetWrapper bitSets = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataChunk, 0, dataChunk.length);
					if(isVirgin){
						destination.setDocumentSequences(bitSets);
						isVirgin = false;
						continue;					
					}
					else{
						BitSetOrSet source = new BitSetOrSet();
						source.setDocumentSequences(bitSets);
						destination.or(source);
					}
				}
			}
			return destination.getDocumentSequences();

		} catch (Exception e) {
			String msg = "Error while processing query [" + query + "]\n";
			msg = msg + "Found Data Chunk\t" + (( null == dataChunk) ? "None" : new String(dataChunk) );
			IdSearchLog.l.fatal(this.getClass().getName() + ":\t"  + msg);
			e.printStackTrace();
			throw new IOException(msg, e);
		} 
	}
	
	private final BitSetWrapper readStorageTextIdsBitset(final boolean checkForAllWords, 
		final boolean keepPhrase, boolean isCompressed, 
		boolean isCached, final String fieldName) throws IOException{

		BitSetOrSet destination  = new BitSetOrSet();
		String rowIdPrefix = rowId ;

		String query = null;
		try {

			int queryPartLoc = filterQuery.lastIndexOf('|');
			query = ( queryPartLoc < 0 ) ? filterQuery : filterQuery.substring(queryPartLoc + 1);
			int qLen = ( null == query) ? 0 : query.trim().length();  
			if ( 0 == qLen) {
				throw new IOException(filterQuery + " > Query is Null/Blank while processing field " + fieldName );
			}
			Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fieldName);
			QueryParser qp = new QueryParser(Version.LUCENE_36, "K", analyzer);
			
			Query q = null;
			q = qp.parse(query);
			Set<Term> terms = new LinkedHashSet<Term>();
			q.extractTerms(terms);
			boolean isVirgin = true;
			
			if ( keepPhrase && checkForAllWords) {
				int termsT = terms.size();
				if ( termsT == 2 || termsT == 3) {
					Iterator<Term> itr = terms.iterator();
					String phrase = ( termsT == 2 ) ? 
						itr.next() + " " + itr.next() : 
						itr.next() + " " + itr.next() + " " + itr.next();
						
					findATerm(checkForAllWords, isCompressed, isCached,
						destination, rowIdPrefix, isVirgin, phrase);
					BitSetWrapper result = destination.getDocumentSequences();
					int resultT = ( null == result) ? 0 : result.cardinality(); 
					if (  resultT > 0 ) return destination.getDocumentSequences();
				}
			}
			
			for (Term term : terms) {
				if ( DEBUG_ENABLED) IdSearchLog.l.debug("Finding Term :" + term.text());
				String word = term.text();
				 int result = findATerm(checkForAllWords, isCompressed, isCached,
						destination, rowIdPrefix, isVirgin, word);
				 if ( result == -1 ) break;
				 isVirgin = ( 1 == result );
			}
			
			return destination.getDocumentSequences();
			
		} catch (Exception e) {
			String msg = "Error while processing query [" + query + "]\n";
			IdSearchLog.l.fatal(this.getClass().getName() + ":\t"  + msg);
			e.printStackTrace();
			throw new IOException(msg, e);
		} 
	}

	private int findATerm(final boolean checkForAllWords,
			boolean isCompressed, boolean isCached, BitSetOrSet destination,
			String rowIdPrefix, boolean isVirgin, String word)
			throws IOException, FederatedSearchException {
		

		String currentRowId = rowIdPrefix + word;
		
		/**
		 * Check the cache
		 */
		byte[] dataChunk = null;
		int size = 0;
		try {
			dataChunk = DataBlock.getBlock(tableName, currentRowId, isCached);
			size = ( null == dataChunk) ? 0 : dataChunk.length;
			if ( isCompressed && size > 0 ) {
				byte[] dataChunkUncompressed = Snappy.uncompress(dataChunk, 0 , dataChunk.length);
				dataChunk = dataChunkUncompressed;
			}
			
		} catch (Exception e) {
			String msg = "Found Data Chunk\t" + (( null == dataChunk) ? "None" : new String(dataChunk) );
			IdSearchLog.l.fatal(this.getClass().getName() + ":\t"  + msg);
			e.printStackTrace();
			throw new IOException(msg, e);
		} 
		

		if ( checkForAllWords ) {
			
			if ( size == 0 ) {
				destination.clear();
				return -1;
			}
			
			BitSetWrapper bitSets = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataChunk, 0, dataChunk.length);
			
			if ( isVirgin ) {
				destination.setDocumentSequences(bitSets);
				isVirgin = false;
				return (isVirgin ? 1 : 0 );					
			}
			
			BitSetOrSet source = new BitSetOrSet();
			source.setDocumentSequences(bitSets);
			destination.and(source);
			
		} else {
			if ( size == 0 ) return (isVirgin ? 1 : 0 );					

			BitSetWrapper bitSets = SortedBytesBitset.getInstanceBitset().
				bytesToBitSet(dataChunk, 0, dataChunk.length);
			
			if(isVirgin){
				destination.setDocumentSequences(bitSets);
				isVirgin = false;
				return (isVirgin ? 1 : 0 );
			}
			else{
				BitSetOrSet source = new BitSetOrSet();
				source.setDocumentSequences(bitSets);
				destination.or(source);
			}
		}
		return (isVirgin ? 1 : 0 );
	}	
	
	public final void setFieldTypeCodes(final Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public final void setDocumentTypeCodes(final Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}	
}