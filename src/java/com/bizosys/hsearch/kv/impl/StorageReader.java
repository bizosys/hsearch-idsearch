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
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.iq80.snappy.Snappy;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.dao.KvRowReaderFactory;
import com.bizosys.hsearch.kv.dao.MapperKV;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.storage.DatabaseReader;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.util.Hashing;
import com.bizosys.unstructured.AnalyzerFactory;

/**
 * The reader base class.
 * Reads the matching ids and the values.
 * @author shubhendu
 *
 */
public class StorageReader implements Callable<Map<Integer, Object>> {

	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = IdSearchLog.l.isInfoEnabled();

	public KVDocIndexer indexer = new KVDocIndexer();

	public String tableName;
	public String rowId;
	public byte[] matchingIdsB;
	public String filterQuery;
	public HSearchProcessingInstruction instruction = null;
	public String fieldName = null;
	public BitSetOrSet matchingIds = null;


	boolean isCachable = true;

	/**
	 * 
	 * @param tableName
	 * @param rowId
	 * @param filterQuery
	 * @param instruction
	 * @param isCachable
	 */
	public StorageReader(final String tableName, final String rowId, final String filterQuery, 
			final HSearchProcessingInstruction instruction, boolean isCachable) {
		this.tableName = tableName;
		this.rowId = rowId;
		this.filterQuery = filterQuery;
		this.instruction = instruction;
		this.isCachable = isCachable;
	}

	/**
	 * 
	 * @param tableName
	 * @param rowId
	 * @param matchingIds
	 * @param matchingIdsB
	 * @param fieldName
	 * @param filterQuery
	 * @param instruction
	 * @param isCachable
	 */
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

	/**
	 * Reads the values for a given query and returns the map of key value pair. 
	 * @return
	 * @throws IOException
	 */
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
			KvRowReaderFactory readerFactory = KvRowReaderFactory.getInstance();
			DatabaseReader reader = readerFactory.getReader(this.isCachable); 
			if(null == matchingIdsB){
				finalResult = reader.readRow(tableName, rowId.getBytes(), 
						compute, this.filterQuery, instruction);
			} else {
				finalResult = reader.readStoredProcedure(tableName, rowId.getBytes(), 
						compute, matchingIdsB, matchingIds.getDocumentSequences(), filterQuery, instruction);
			}

			if(DEBUG_ENABLED){
				long end = System.currentTimeMillis();
				if(null != finalResult)
					IdSearchLog.l.debug(rowId + " Fetch time " + (end - start) +" for " + finalResult.size() + " records");
			}

		} catch (Exception e) {
			String msg = e.getMessage() + "\nFor rowid = "+ rowId + " query = " + filterQuery;
			IdSearchLog.l.fatal("ReadStorage Exception " + msg , e );
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
	public final BitSetOrSet readStorageIds() throws IOException {
		byte[] data = null;
		try {

			BitSetWrapper bitSets = null;
			DatabaseReader reader = KvRowReaderFactory.getInstance().getReader(isCachable); 
			if ( isCachable) {
				byte[] dataA = reader.readRowBlob(tableName, rowId.getBytes());
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
				if ( DEBUG_ENABLED ) IdSearchLog.l.debug("reader.readStoredProcedureBlob:rowId " + rowId);

				BitSetWrapper bits = ( null == matchingIds) ? null : matchingIds.getDocumentSequences(); 

				data = reader.readStoredProcedureBlob(tableName, rowId.getBytes(), null, null, 
						bits, filterQuery, instruction);

				Collection<byte[]> dataL = SortedBytesArray.getInstanceArr().parse(data).values();
				byte[] dataChunk = dataL.isEmpty() ? null : dataL.iterator().next();
				if ( null != dataChunk ) {
					bitSets = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataChunk, 0, dataChunk.length);
				}
			}

			if ( null == bitSets)  bitSets = new BitSetWrapper(0);
			BitSetOrSet dest = new BitSetOrSet();
			dest.setDocumentSequences(bitSets);
			return dest;

		} catch (Exception e) {
			String msg = "Row Id = " + rowId + "\n"; 
			msg = msg + "Filter Query = " + filterQuery + "\n"; 
			msg = msg + (( null == data) ? "Null Data" :  new String(data)) + "\n";
			msg = "Error :" + e.getMessage();
			IdSearchLog.l.fatal(msg);
			IOException ioException = new IOException(msg, e);
			throw ioException;
		}
	}

	/**
	 * Returns the ids for analyzed field.
	 * @param fld
	 * @param checkForAllWords
	 * @param fieldName
	 * @param enableNGram
	 * @return
	 * @throws IOException
	 */
	public final BitSetOrSet readStorageTextIds(final Field fld,final String fieldQuery,
			boolean checkForAllWords, final String fieldName, boolean enableNGram, boolean checkExactPhrase) throws IOException{

		if ( fld.isRepeatable ) {
			return readStorageTextIdsBitset(checkForAllWords, fieldQuery, 
					fld.isBiWord, fld.isTriWord, fld.isCompressed, fld.isCachable, fieldName, enableNGram, checkExactPhrase);
		} else {
			return readStorageTextIdsSet(checkForAllWords, fieldName, fieldQuery);
		}
	}

	/**
	 * Returns the ids for analyzed field that is not repeatable.
	 * @param checkForAllWords
	 * @param fieldName
	 * @return
	 * @throws IOException
	 */
	private final BitSetOrSet readStorageTextIdsSet(final boolean checkForAllWords, 
			final String fieldName, String fieldQuery) throws IOException{

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
		try {

			Map<String, Integer> dTypes = new HashMap<String, Integer>(1);
			dTypes.put(docType, 1);
			setDocumentTypeCodes(dTypes);

			Map<String, Integer> fTypes= new HashMap<String, Integer>(1);
			fTypes.put(fieldType, 1);
			setFieldTypeCodes(fTypes);

			Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fieldName);
			TokenStream stream = analyzer.tokenStream(fieldName, new StringReader(fieldQuery));
			CharTermAttribute termAttribute = stream .getAttribute(CharTermAttribute.class);
			
			Set<String> terms = new LinkedHashSet<String>();
			
			while (stream .incrementToken()) {
				terms.add(termAttribute.toString());
			}

			String docTypeCode = "*".equals(docType) ? "*" :
				new Integer(DocumentTypeCodes.getInstance().getCode(docType)).toString();

			String fldTypeCode = "*".equals(fieldType) ? "*" :
				new Integer(FieldTypeCodes.getInstance().getCode(fieldType)).toString();

			for (String term : terms) {
				if ( DEBUG_ENABLED) {
					IdSearchLog.l.debug("Finding Term :" + term);
				}

				hash = Hashing.hash(term);
				wordHash = new Integer(hash).toString();
				sb.delete(0, sb.length());
				fieldQuery = sb.append(docTypeCode).append('|').append(fldTypeCode).append('|')
						.append('*').append('|').append(hash).append('|').append("*|*").toString();
				sb.delete(0, sb.length());
				currentRowId = mergeid + "_" + wordHash.charAt(0) + "_" + wordHash.charAt(wordHash.length() - 1);

				ComputeKV compute = new ComputeKV();
				compute.kvType = (instruction.getOutputType() == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : instruction.getOutputType();
				compute.kvRepeatation = instruction.getProcessingHint().startsWith("true");
				compute.isCompressed = instruction.getProcessingHint().endsWith("true");
				byte[] data = KvRowReaderFactory.getInstance().getReader(this.isCachable).readStoredProcedureBlob(
						tableName, currentRowId.getBytes(), compute, null, null, filterQuery, instruction);					

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
			return destination;

		} catch (Exception e) {
			String msg = "Error while processing query [" + fieldQuery + "]\n";
			msg = msg + "Found Data Chunk\t" + (( null == dataChunk) ? "None" : new String(dataChunk) );
			IdSearchLog.l.fatal(this.getClass().getName() + ":\t"  + msg);
			e.printStackTrace();
			throw new IOException(msg, e);
		} 
	}

	/**
	 * Returns the ids for analyzed field that is repeatable.
	 * @param checkForAllWords
	 * @param biWord
	 * @param triWord
	 * @param isCompressed
	 * @param isCached
	 * @param fieldName
	 * @param enableNGram
	 * @return
	 * @throws IOException
	 */
	private final BitSetOrSet readStorageTextIdsBitset(final boolean checkForAllWords, final String fieldQuery, 
			final boolean biWord, final boolean triWord, boolean isCompressed, 
			boolean isCached, final String fieldName, boolean enableNGram, boolean checkExactPhrase) throws IOException{

		BitSetOrSet destination  = new BitSetOrSet();
		String rowIdPrefix = rowId ;

		try {

			Analyzer analyzer = AnalyzerFactory.getInstance().getAnalyzer(fieldName);
			TokenStream stream = analyzer.tokenStream(fieldName, new StringReader(fieldQuery));
			CharTermAttribute termAttribute = stream .getAttribute(CharTermAttribute.class);
			
			Set<String> terms = new LinkedHashSet<String>();
			
			while (stream .incrementToken()) {
				terms.add(termAttribute.toString());
			}
				
			
			
			int termsT = terms.size();

			if (enableNGram) {

				if ( DEBUG_ENABLED ) IdSearchLog.l.debug("NGRam Explosion");

				int subsequenceLen = 1;

				if ( biWord ) subsequenceLen=2;
				else if ( triWord ) subsequenceLen=3;

				/**
				 * There may be an pentalty on performance.
				 * Don't allow total search phrases > 10 
				 */
				if ( triWord && (termsT > 4) ) subsequenceLen=2;
				if ( (subsequenceLen==2) && (termsT > 5)  ) subsequenceLen=1;

				/**
				 * "red party gown"
				 * "party gown dress"
				 * "red party"
				 * "party gown"
				 * "gown dress"
				 * "red"
				 * "party"
				 * "gown"
				 * "dress"
				 */
				List<String> phrases = new ArrayList<String>();
				StringBuilder sb = new StringBuilder(1024);

				String[] termsA = new String[terms.size()] ;
				terms.toArray(termsA);

				for (int subSequence=subsequenceLen; subSequence> 0; subSequence--) {
					if ( subSequence <= 0 ) break;

					for ( int wordPosition=0; wordPosition<= termsT- subSequence; wordPosition++ ) {

						for (int pos=0; pos< subSequence; pos++) {
							if ( pos > 0) sb.append(' ');
							sb.append(termsA[wordPosition+pos]);
						}
						phrases.add(sb.toString());
						sb.setLength(0);
					}
				}

				for (String phrase : phrases) {
					BitSetOrSet phraseMatches = new BitSetOrSet();

					findATerm(checkForAllWords, isCompressed, isCached,
							phraseMatches, rowIdPrefix, phrase, false);
					destination.orQueryWithFoundIds.put(phrase, phraseMatches);
					destination.or(phraseMatches);
				}

				if ( DEBUG_ENABLED ) IdSearchLog.l.debug(
						"NGram Query OR trace > " + destination.orQueryWithFoundIds.toString());

				return destination;

			} else {

				if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Normal Query processing");

				//check for all words 

				BitSetOrSet highRanked = null;
				switch (termsT) {
				case 2:{

					/**
					 * All 2 words are consecutive
					 */
					if ( biWord ) {
						Iterator<String> itr = terms.iterator();
						String phrase = itr.next() + " " + itr.next();

						findATerm(checkForAllWords, isCompressed, isCached,
								destination, rowIdPrefix, phrase, true);
						BitSetWrapper result = destination.getDocumentSequences();
						int resultT = ( null == result) ? 0 : result.cardinality(); 
						if (  resultT > 0 || checkExactPhrase) return destination;
					}
					
					
					/*
					 * Biword search result is 0 so search for all words.
					 */
						return searchForAllWord(terms, checkForAllWords, isCompressed, isCached, destination, rowIdPrefix);
				}
				case 3:{

					/**
					 * All 3 words are consecutive
					 */
					Iterator<String> itr = terms.iterator();
					String word1 = itr.next();
					String word2 = itr.next();
					String word3 = itr.next();

					if ( triWord ) {
						String phrase = word1 + " " + word2 + " " + word3;
						findATerm(checkForAllWords, isCompressed, isCached,
								destination, rowIdPrefix, phrase, true);
						BitSetWrapper result = destination.getDocumentSequences();
						int resultT = ( null == result) ? 0 : result.cardinality(); 
						if (  resultT > 0 || checkExactPhrase) return destination;
					}

					/**
					 * If Check for all words is true minimum required result is 1 for three words
					 * else minimum required result is 0
					 */
					int requiredMinResult = checkForAllWords ? 1 : 0;

					/**
					 * 2 words are consecutive, take them and apply findAll on them
					 */
					if ( biWord ) {

						String biword1 = word1 + " " + word2;
						String biword2 = word2 + " " + word3;
						String biword3 = word1 + " " + word3;

						highRanked = new BitSetOrSet();
						String[] phrases = new String[] {biword1, biword2, biword3};

						int found = 0;
						for (String phrase : phrases) {
							int result = findATerm(false, isCompressed, isCached,
									highRanked, rowIdPrefix, phrase, false);
							if ( result > 0 ) found++;
						}

						if ( found > requiredMinResult || checkExactPhrase) return highRanked;

					} 

					/*
					 * Biword and Triword search result is 0 so search for all words.
					 */
					return searchForAllWord(terms, checkForAllWords, isCompressed, isCached, destination, rowIdPrefix);
				}
				case 4 : {
					Iterator<String> itr = terms.iterator();
					String word1 = itr.next();
					String word2 = itr.next();
					String word3 = itr.next();
					String word4 = itr.next();
					int requiredMinResult = 0;
					if( triWord ){
						
						requiredMinResult = checkForAllWords ? 1 : 0;
						
						String triword1 = word1 + " " + word2 + " " + word3;
						String triword2 = word1 + " " + word3 + " " + word4;
						String triword3 = word2 + " " + word3 + " " + word4;
						highRanked = new BitSetOrSet();
						String[] phrases = new String[] {triword1, triword2, triword3};

						int found = 0;
						for (String phrase : phrases) {
							int result = findATerm(false, isCompressed, isCached,
									highRanked, rowIdPrefix, phrase, false);
							if ( result > 0 ) found++;
						}
						
						if ( found > requiredMinResult || checkExactPhrase) return highRanked;
					}
					
					if( biWord ){
						
						requiredMinResult = checkForAllWords ? 2 : 0;
						
						String biword1 = word1 + " " + word2;
						String biword2 = word1 + " " + word3;
						String biword3 = word1 + " " + word4;
						String biword4 = word2 + " " + word3;
						String biword5 = word2 + " " + word4;
						
						highRanked = new BitSetOrSet();
						String[] phrases = new String[] {biword1, biword2, biword3, biword4, biword5};

						int found = 0;
						for (String phrase : phrases) {
							int result = findATerm(false, isCompressed, isCached,
									highRanked, rowIdPrefix, phrase, false);
							if ( result > 0 ) found++;
						}
						
						if ( found > requiredMinResult || checkExactPhrase) return highRanked;
						
					}
					
					/*
					 * Biword and Triword search result is 0 so search for all words.
					 */

					return searchForAllWord(terms, checkForAllWords, isCompressed, isCached, destination, rowIdPrefix);
					
				}
				default:{
					/*
					 * Biword and Triword is not enabled so search for all words.
					 */

					return searchForAllWord(terms, checkForAllWords, isCompressed, isCached, destination, rowIdPrefix);
				}
				}
			}
		} catch (Exception e) {
			String msg = "Error while processing query [" + fieldQuery + "]\n";
			IdSearchLog.l.fatal(this.getClass().getName() + ":\t"  + msg);
			e.printStackTrace();
			throw new IOException(msg, e);
		} 
	}
	
	/**
	 * Searches all terms in a given set and returns the matching ids.
	 * @param terms
	 * @param checkForAllWords
	 * @param isCompressed
	 * @param isCached
	 * @param destination
	 * @param rowIdPrefix
	 * @return
	 * @throws IOException
	 * @throws FederatedSearchException
	 */
	public BitSetOrSet searchForAllWord(Set<String> terms, boolean checkForAllWords, boolean isCompressed, boolean isCached, BitSetOrSet destination, String rowIdPrefix) throws IOException, FederatedSearchException{
		/**
		 * Check for all words
		 */
		boolean isFirstTime = true;
		for (String term : terms) {
			if ( DEBUG_ENABLED) IdSearchLog.l.debug("Finding Term :" + term);
			String word = term;
			int result = findATerm(checkForAllWords, isCompressed, isCached,
					destination, rowIdPrefix, word, isFirstTime);
			isFirstTime = false;
			if ( result == 0 && checkForAllWords) {
				destination.clear();
				break;
			}
		}
		return destination;
	}

	/**
	 * 
	 * @param checkForAllWords
	 * @param isCompressed
	 * @param isCached
	 * @param destination
	 * @param rowIdPrefix
	 * @param word
	 * @param isVirgin
	 * @return Not found -1, If Found, 0
	 * @throws IOException
	 * @throws FederatedSearchException
	 */
	private int findATerm(final boolean checkForAllWords,
			boolean isCompressed, boolean isCached, BitSetOrSet destination,
			String rowIdPrefix,  String word, boolean isFirstTime) throws IOException, FederatedSearchException {


		String currentRowId = rowIdPrefix + word;

		/**
		 * Check the cache
		 */
		byte[] dataChunk = null;
		int size = 0;
		try {
			dataChunk = KvRowReaderFactory.getInstance().getReader(
					this.isCachable).readRowBlob(tableName, currentRowId.getBytes());
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

			/**
			 * Nothing found
			 */
			if ( size == 0 ) {
				destination.clear();
				return size;
			}

			/**
			 * Something is found
			 */
			BitSetWrapper bitSets = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataChunk, 0, dataChunk.length);

			/**
			 * If it is first time, just set it and later do and.
			 */
			if ( isFirstTime ) {
				destination.setDocumentSequences(bitSets);
				return bitSets.cardinality();					
			} else {
				if ( null != destination.getDocumentSequences()) {
					destination.getDocumentSequences().and(bitSets);
					return bitSets.cardinality();
				}
			}

			if ( DEBUG_ENABLED) 
				IdSearchLog.l.debug("StorageReader: After and operation :" + word + " with source size : " + bitSets.size() + " and dest size :" + destination.size());			

		} else {
			if ( size == 0 ) return size;

			BitSetWrapper bitSets = SortedBytesBitset.getInstanceBitset().
					bytesToBitSet(dataChunk, 0, dataChunk.length);

			if ( DEBUG_ENABLED) 
				IdSearchLog.l.debug("StorageReader: Doing or operation for :" + word + " with source size : " + bitSets.size() + " and dest size :" + destination.size());			

			if(isFirstTime){
				destination.setDocumentSequences(bitSets);
				return bitSets.cardinality();					
			} else {
				if ( null == destination.getDocumentSequences()) destination.setDocumentSequences(bitSets);
				else destination.getDocumentSequences().or(bitSets);
				return bitSets.cardinality();					
			}
		}
		return 0;					

	}	

	public final void setFieldTypeCodes(final Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public final void setDocumentTypeCodes(final Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}	
}