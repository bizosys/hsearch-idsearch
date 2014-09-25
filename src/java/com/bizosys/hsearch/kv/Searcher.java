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

package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.FederatedSearch;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;
import com.bizosys.hsearch.functions.GroupSorter;
import com.bizosys.hsearch.functions.GroupSorter.GroupSorterSequencer;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NV;
import com.bizosys.hsearch.hbase.RecordScalar;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.dao.KvRowReaderFactory;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmptyImpl;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.kv.impl.Datatype;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.StorageReader;
import com.bizosys.hsearch.kv.impl.TypedObject;
import com.bizosys.hsearch.kv.indexing.KVIndexer;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.hsearch.util.ShutdownCleanup;

/**
 * 
 * The base class used for searching in a hsearch index.
 *
 */
public class Searcher {

	public String dataRepository = "";
	public String schemaRepositoryName = "";
	
	public static final Pattern patternExtraWhitespaces = Pattern.compile("\\s+");
	public static final Pattern patternBooleans = Pattern.compile("( AND | OR | NOT )");
	public static final Pattern patternComma = Pattern.compile(",");
	public static final Pattern patternBracketsOutsideQuotes = Pattern.compile("(\\(|\\))(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	public static final Pattern patternRemoveQuotes = Pattern.compile("\"");
	
	private Set<KVRowI> resultset = null; //Replaced with LinkedHashSet for sorting
	private Map<String, Map<Object, FacetCount>> facetsMap = null;
	private Map<String, List<HsearchFacet>> pivotFacetsMap = null;
	private KVDataSchemaRepository repository = KVDataSchemaRepository.getInstance();
	
	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = IdSearchLog.l.isInfoEnabled();
	
	private boolean checkForAllWords = false;
	private boolean checkExactPhrase = false;
	private SearcherPluginOffset pageCalculator = new SearcherPluginOffset();	
	private ISearcherPlugin plugIn = null;	
	
	private Map<Integer,KVRowI> joinedRows = null;
	
	private int internalFetchLimit = 10000;

	private boolean isNGramEnabled = false;
	
	private boolean isDefaultRankingEnabled = false;

	/**
	 * Constructor - Requires the schema and the processed field mapping.
	 * @param schemaName - The Unique Schema name 
	 * @param fm - The Field Mapping Object
	 */
	public Searcher(final String schemaName, final FieldMapping fm){
		this(schemaName, fm.tableName, fm);
	}
	
	/**
	 * Constructor - Schema, Processed Fields and overriden storage table.
	 * @param schemaName - The Unique Schema name 
	 * @param dataRepository- Storage Folder/Table name
	 * @param fm - The Field Mapping Object
	 */
	public Searcher(final String schemaName, final String dataRepository, final FieldMapping fm){
		this.schemaRepositoryName = schemaName;
		this.dataRepository = dataRepository;
		this.repository.add(schemaRepositoryName, fm);
		this.resultset = new LinkedHashSet<KVRowI>();
		this.facetsMap = new HashMap<String, Map<Object,FacetCount>>();
	}

	/**
	 * Should we look for all words during the search query
	 * @param checkForAllWords - True to include all words
	 */
	public final void setCheckForAllWords(final boolean checkForAllWords) {
		this.checkForAllWords = checkForAllWords;
	}

	/**
	 * Should we look for exact phrase during the search query
	 * @param checkExactPhrase - True to search for exact phrase
	 */
	public final void setCheckExactPhrase(final boolean checkExactPhrase) {
		this.checkExactPhrase = checkExactPhrase;
	}

	/**
	 * Set the plugin class
	 * @param plugIn - The plugin class for the callbacks
	 */
	public final void setPlugin(ISearcherPlugin plugIn) {
		this.plugIn = plugIn;
	}	

	/**
	 * Set the previous facet results
	 * @param map of facets 
	 */
	public final void setPreviousFacet(Map<String, Map<Object, FacetCount>> previousFacet) {
		this.facetsMap.putAll(previousFacet);
	}	

	/**
	 * Sets the internal fetch limit
	 * @param limit - The limit
	 */
	public final void setInternalFetchLimit(int limit) {
		this.internalFetchLimit = limit;
	}	
	
	/**
	 * Is NGram lookup enabled
	 * @param enabled True to enable and False to disable
	 */
	public final void setNGram(boolean enabled) {
		this.isNGramEnabled = enabled;
	}
	
	/**
	 * Enable the internal ranking mechanism.
	 * @param enabled True to enable and False to disable
	 */
	public final void setDefaultRanking(boolean enabled) {
		this.isDefaultRankingEnabled = enabled;
	}
	
	
	/**
	 * The pagination calculator.
	 * @param offset - The start row sequence of the page
	 * @param pageSize - The page size
	 */
	public final void setPage(int offset, int pageSize) {
		this.pageCalculator.set(offset, pageSize);
	}	

	/**
	 * Get all the merge / partition keys
	 * @return	- List of all partition keys
	 * @throws IOException
	 */
	public final Set<String> getMergeIds() throws IOException {
		
		Set<String> mergeKeys = new HashSet<String>();
		byte[] mergeKeysB = HReader.getScalar(dataRepository, KVIndexer.FAM_NAME, KVIndexer.COL_NAME, KVIndexer.MERGEKEY_ROW.getBytes());
		SortedBytesString.getInstance().parse(mergeKeysB).addAll(mergeKeys);

		return mergeKeys;
	}	
	
	/**
	 * Search by the regular expression on a partition key
	 * @param mergeIdPattern - Which all partition key to match
	 * @param selectQuery - Comma separated select fields
	 * @param whereQuery - The lucene style search query
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out
	 * @throws IOException 
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	public final void searchRegex(final String mergeIdPattern, final String selectQuery, String whereQuery,
			KVRowI blankRow, IEnricher... enrichers) 
		throws IOException, InterruptedException, ExecutionException, FederatedSearchException, NumberFormatException, ParseException  {

		Set<String> mergeIds = getMergeIds();
		List<String> matchedMergeId = new ArrayList<String>();
		Pattern patternMergeIds = Pattern.compile(mergeIdPattern);
		for (String mergeId : mergeIds) {
			if(patternMergeIds.matcher(mergeId).matches())
				matchedMergeId.add(mergeId);
		}
		search(matchedMergeId, selectQuery, whereQuery, blankRow, enrichers);
		
	}
	
	/**
	 * Search on multuple known merge ids
	 * @param mergeIds - All merge ids
	 * @param selectQuery - Comma separated select fields
	 * @param whereQuery - The lucene style search query
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws ParseException
	 */
	public final void search(final Collection<String> mergeIds, final String selectQuery, String whereQuery,
			KVRowI blankRow, IEnricher... enrichers) throws NumberFormatException, IOException, InterruptedException, ExecutionException, FederatedSearchException, ParseException{

		for (String mergeId : mergeIds) {
			search(mergeId, selectQuery, whereQuery, blankRow, enrichers);
			if ( null != this.joinedRows) this.joinedRows.clear();
		}
	}

	
	/**
	 * Search inside a partition 
	 * @param mergeId - Search on the exact partition key
	 * @param selectQuery - Comma separated select fields
	 * @param whereQuery - The lucene style search query
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	public final void search(final String mergeId, final String selectQuery, String whereQuery, 
			final KVRowI blankRow, final IEnricher... enrichers) 
			throws IOException, InterruptedException, ExecutionException, FederatedSearchException, NumberFormatException, ParseException {
		
		this.search(mergeId, selectQuery, whereQuery, null, blankRow, enrichers);
	}
	
	
	/**
	 * Search inside a skewed partition 
	 * @param mergeId - Search on the exact partition key
	 * @param selectQuery - Comma separated select fields
	 * @param whereQuery - The lucene style search query
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	public final void searchSkew(final String mergeId, final String selectQuery, String whereQuery,
			KVRowI blankRow, IEnricher... enrichers) 
		throws IOException, InterruptedException, ExecutionException, FederatedSearchException, NumberFormatException, ParseException  {

		/**
		 * If Skew point is enabled get the first and last mergeid
		 */

		FieldMapping fm = repository.get(schemaRepositoryName).fm;
		if(fm.skewPoint > 0){

			NV kv = new NV(fm.familyName.getBytes(), "1".getBytes());
			String row = mergeId + KVIndexer.INCREMENTAL_ROW;
			RecordScalar maxRecordReader = new RecordScalar(row.getBytes(), kv);
			HReader.getScalar(fm.tableName, maxRecordReader);
			int maximumRecords = new Long(Storable.getLong(0, maxRecordReader.kv.data)).intValue();
			int totalBucket = maximumRecords / fm.skewPoint;
			
			for(int i = 0 ; i < totalBucket; i++) {
				String aSkewBucket = (mergeId + "_" + i);
				search(aSkewBucket, selectQuery, whereQuery, blankRow, enrichers);
				if ( null != this.joinedRows) this.joinedRows.clear();
			}
			
		} else {
			search(mergeId, selectQuery, whereQuery, blankRow, enrichers); 
		}
	}
	
	/**
	 * Search, Build Facet and Sort the result. Sorting happens when all values are out.
	 * @param mergeId - Search on the exact partition key
	 * @param selectQuery - Comma separated select fields
	 * @param whereQuery - The lucene style search query
	 * @param facetFields - Comma separated Facet fields : Null or String.Empty if none
	 * @param sortFields - Comma separated sort fields : Null or String.Empty if none
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out (Optional)
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	public final void search(final String mergeId, final String selectQuery, String whereQuery,String facetFields,String sortFields, 
			final KVRowI blankRow, final IEnricher... enrichers) 
			throws IOException, InterruptedException, ExecutionException, FederatedSearchException, NumberFormatException, ParseException {
		
		int sortFieldsT = ( null == sortFields ) ? 0 : sortFields.trim().length();
		Map<Integer, BitSetWrapper> rankBuckets = null;

		/**
		 * Disable Page Calculation and Just find matching Fields.
		 */
		if ( 0 == sortFieldsT) {
			this.pageCalculator.disabled = false; //Disable pagination
			rankBuckets = this.search(mergeId, selectQuery, whereQuery,facetFields, blankRow, enrichers);
			
			if ( null == rankBuckets )
				if ( null != this.plugIn) this.plugIn.onFacets(mergeId, this.facetsMap);
			
		} else {
			StringBuilder sortSelect = new StringBuilder(sortFields.length());
			for (char aChar : sortFields.toCharArray()) {
				if ( aChar == '^') continue;
				sortSelect.append(aChar);
			}
			this.pageCalculator.disabled = true; //Disable pagination
			rankBuckets = this.search(
				mergeId, sortSelect.toString(), whereQuery, facetFields, blankRow, enrichers);

			if ( null == rankBuckets )
				if ( null != this.plugIn) this.plugIn.onFacets(mergeId, this.facetsMap);

			this.pageCalculator.disabled = false; //Enable Pagination

			/**
			 * Provide the last change to influence the sort fields based on the internal rank buckets
			 */
			if ( null != this.plugIn) {
				boolean shouldShortAgain = this.plugIn.beforeSort(mergeId, sortFields, this.resultset, rankBuckets);
				if ( shouldShortAgain ) this.sort(patternComma.split(sortFields));
			} else {
				this.sort(patternComma.split(sortFields));
			}
			if ( null != this.plugIn) this.plugIn.afterSort(mergeId, this.resultset);
		}
		
		if ( sortFieldsT == 0 ) {
			if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Default Rank Sorting  ");
			this.resultset = this.pageCalculator.defaultRankSortPage(this.resultset, rankBuckets);
			
		} else {
			int resultsetT = (null == this.resultset) ? 0 : this.resultset.size();  
			if ( 0 == resultsetT) return;
			if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Paginate, Select fields and Resort : " + sortFields);
			this.getValuesAfterSorting(mergeId, selectQuery, blankRow, enrichers);
			this.sort(patternComma.split(sortFields)); //Re-Sort
		}
	}

	/**
	 * Search and build facet. If the sorting is enabled, it will return the default sorted list.
	 * @param mergeId - Search on the exact partition key
	 * @param selectQuery - Comma separated select fields
	 * @param whereQuery - The lucene style search query
	 * @param facetFields - Comma separated Facet fields : Null or String.Empty if none
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out (Optional)
	 * @return Default sorted Document Ids
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	public final Map<Integer, BitSetWrapper> search(final String mergeId,  
			final String selectQuery, String whereQuery, String facetFields,
			final KVRowI blankRow, final IEnricher... enrichers) 
			throws IOException, InterruptedException, ExecutionException, FederatedSearchException, NumberFormatException, ParseException {
		
		BitSetOrSet matchIds = null;
		int matchIdsFoundT = 0;
		boolean isWhereQueryEmpty = ( null == whereQuery) ? true : (whereQuery.length() == 0);
		boolean isFacetFieldEmpty = ( null == facetFields) ? true : (facetFields.length() == 0);
		
		/**
		 * Find Matching Ids - START
		 */
		
		Map<String, QueryPart> whereParts = new HashMap<String, QueryPart>();
		if(!isWhereQueryEmpty){
			whereQuery = parseWhereQuery(mergeId, whereQuery, whereParts);
			matchIds = getIds(mergeId, whereQuery, whereParts);
			matchIdsFoundT = ( null == matchIds) ? 0 : ( null == matchIds.getDocumentSequences()) ? 0 : matchIds.getDocumentSequences().cardinality();
			if(0 == matchIdsFoundT) return null;
		}

		/**
		 * Row Joining - START
		 */
		if ( DEBUG_ENABLED) IdSearchLog.l.debug("Total Ids Found (Before Join) :" + matchIdsFoundT);
		int joinedRowSize = (null == matchIds) ?  64 : matchIdsFoundT;
		if ( null == this.joinedRows)
			this.joinedRows = new HashMap<Integer, KVRowI>(joinedRowSize);
		else this.joinedRows.clear();

		if ( null != matchIds) {
			if ( null != this.plugIn) this.plugIn.onJoin(
				mergeId, matchIds.getDocumentSequences(), whereParts, this.joinedRows);			
			matchIdsFoundT = ( null == matchIds) ? 0 : ( null == matchIds.getDocumentSequences()) ? 0 : matchIds.getDocumentSequences().cardinality();
			if(0 == matchIdsFoundT) return null;
		}
		/**
		 * Row Joining - END |   Find Matching Ids - END | Perform Default Ranking - Default cutoff limit START
		 */
		Map<Integer, BitSetWrapper> rankBuckets = null;
		if ( this.isDefaultRankingEnabled ) {

			Collection<BitSetOrSet> orQBitsL = matchIds.orQueryWithFoundIds.values();
			List<BitSetWrapper> tempBits = new ArrayList<BitSetWrapper>(); 
			for ( BitSetOrSet bsos : orQBitsL ) {
				BitSetWrapper tempBit = bsos.getDocumentSequences();
				if ( null == tempBit) continue;
				//Remove from tempBits - All the non final bits
				tempBit.and (matchIds.getDocumentSequences()); 
				tempBits.add (tempBit);
			}
			
			rankBuckets = PhraseWeightComputation.calculatePhraseWeight(tempBits);
			PhraseWeightComputation.dedupBuckets(rankBuckets);

		} 
		
		if ( this.internalFetchLimit > 0 ) 
			PhraseWeightComputation.internalFetchLimitTrimming(this.internalFetchLimit, rankBuckets, matchIds);
		
		
		matchIdsFoundT = ( null == matchIds) ? 0 : ( null == matchIds.getDocumentSequences()) ? 0 : matchIds.getDocumentSequences().cardinality();
		if(0 == matchIdsFoundT) return null;
		if ( DEBUG_ENABLED) IdSearchLog.l.debug(
			"Default cut off limit :" + this.internalFetchLimit + "\t" + " , Found : " + matchIds.size());
		
		/**
		 * Default cutoff limit END, Find Facets - START
		 */

		if(!isFacetFieldEmpty){
			if ( 0 == matchIdsFoundT) 
				createFacetCount(null, mergeId, facetFields);
			else
				createFacetCount(matchIds.getDocumentSequences(), mergeId, facetFields);

			if ( null != this.plugIn) this.plugIn.onFacets(mergeId, this.facetsMap);
		}

		/**
		 * We don't have values for the pagination.
		 */
		if ( ! this.pageCalculator.disabled) {
			if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Before Select : Keep Page :" + this.pageCalculator.disabled);
			this.pageCalculator.keepPage(matchIds.getDocumentSequences(),rankBuckets);
			if ( null != this.plugIn) 	{
				this.plugIn.beforeSelect(mergeId, matchIds.getDocumentSequences());
			}
		}

		int matchIdsT = matchIds.getDocumentSequences().cardinality();
		if ( DEBUG_ENABLED) {
			IdSearchLog.l.debug ("After Pagination Total Ids: " + matchIdsT);
		}
		if ( matchIdsT == 0 ) return null;
		
		/**
		 * SELECT FETCH - START
		 */
		if ( null != selectQuery) getValues(mergeId, matchIds, selectQuery, blankRow);
		if ( DEBUG_ENABLED) {
			IdSearchLog.l.debug ("After Select Total Ids : " + this.resultset.size());
		}
		
		
		if ( null != matchIds && ! this.pageCalculator.disabled) {
			if ( null != this.plugIn) 	{
				this.plugIn.afterSelect(mergeId, matchIds.getDocumentSequences(), this.resultset);
			}
		}
		/**
		 * SELECT FETCH - END
		 */
		
		//Perform internal Ranking.

		/**
		 * Enricher - START
		 */
		if ( null != enrichers && null != this.resultset) {
			for (IEnricher enricher : enrichers) {
				if ( null != enricher) enricher.enrich(this.resultset);
			}
		}		
		/**
		 * Enricher - END
		 */
		return rankBuckets;
	}
	
	/**
	 * Get selectable fields after sorting. 
	 * @param mergeId - The partion key
	 * @param selectQuery - Comma separated select fields
	 * @param blankRow - Requires as a template of the value object
	 * @param enrichers - Which all enrichers to apply once the result is out (Optional)
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * @throws FederatedSearchException
	 * @throws NumberFormatException
	 * @throws ParseException
	 */
	private final void getValuesAfterSorting(String mergeId, final String selectQuery, 
			final KVRowI blankRow, final IEnricher... enrichers) 
			throws IOException, InterruptedException, ExecutionException, FederatedSearchException, NumberFormatException, ParseException {
		
		/**
		 * Value fetch
		 */
		if ( null == this.resultset) return;
		if ( null == selectQuery) return; 
		
		BitSetWrapper onePageIds = this.pageCalculator.keepPage(this.resultset);
		
		this.resultset.clear();
		if(null != this.joinedRows)
			joinedRows.clear();

		if ( null != onePageIds) {
			if ( null != this.plugIn) 	{
				this.plugIn.beforeSelectOnSorted(mergeId, onePageIds);
			}
		}
		
		BitSetOrSet onePageIdsBits = new BitSetOrSet();
		onePageIdsBits.setDocumentSequences(onePageIds);
		getValues(mergeId, onePageIdsBits, selectQuery, blankRow);
		
		if ( null != onePageIds) {
			if ( null != this.plugIn) 	{
				this.plugIn.afterSelectOnSorted(mergeId, onePageIds, this.resultset);
			}
		}

		/**
		 * Apply Enricher
		 */
		if ( null != enrichers && null != this.resultset) {
			for (IEnricher enricher : enrichers) {
				if ( null != enricher) enricher.enrich(this.resultset);
			}
		}		
	}
	
	
	@SuppressWarnings("unchecked")
	public final void getValues(final String mergeId, final BitSetOrSet matchIds, final String selectQuery,  
			final KVRowI blankRow, final IEnricher... enrichers) throws IOException, InterruptedException, ExecutionException, FederatedSearchException {

		Map<String, Object> fieldWithDocIdAndValue = null;
		Set<Integer> documentIds = null;
		Set<String> fields = null;
		Map<Integer, Object> result = null;

		boolean isSelectQueryEmpty = ( null == selectQuery) ? true : (selectQuery.length() == 0);
		if(!isSelectQueryEmpty) fieldWithDocIdAndValue = getAllSelectFieldValues(
			mergeId, matchIds, selectQuery);
		
		if(null == fieldWithDocIdAndValue) return;
		
		if ( null == this.joinedRows) {
			int matchIdsT = ( null == matchIds) ? 64 : matchIds.size();
			this.joinedRows = new LinkedHashMap<Integer, KVRowI>(matchIdsT); 
		}
		
		fields = fieldWithDocIdAndValue.keySet();
		KVDataSchema dataSchema = repository.get(schemaRepositoryName);
		KVRowI aRow = null;
		
		for (String field : fields) {

			result = (Map<Integer, Object>) fieldWithDocIdAndValue.get(field);
			if ( null == result) continue;
			documentIds = result.keySet();

			for (Integer id : documentIds) {
				if (this.joinedRows.containsKey(id)){
					aRow = this.joinedRows.get(id);
				} else {
					aRow = blankRow.create(dataSchema, id, mergeId);
					this.joinedRows.put(id, aRow);
				}
				aRow.setValue(field, result.get(id));

			}
		}

		this.resultset.addAll(this.joinedRows.values());
	}

	
	public final BitSetOrSet getIds(final String mergeId, String whereQuery) throws IOException {
		Map<String, QueryPart> whereQueryParts = new HashMap<String, QueryPart>();
		whereQuery = parseWhereQuery(mergeId, whereQuery, whereQueryParts);
		return getIds(mergeId, whereQuery, whereQueryParts);
	}
	
	
	private final BitSetOrSet getIds(final String mergeId, String whereQuery, Map<String, QueryPart> whereQueryParts) throws IOException {

		try {

			FederatedSearch ff = createFederatedSearch();
			ff.setKeepProcessingTracing(this.isDefaultRankingEnabled);
			BitSetOrSet mixedQueryMatchedIds = ff.execute(whereQuery, whereQueryParts);
			
			/**
			 * Remove the delete Ids
			 */
			if (this.repository.get(this.schemaRepositoryName).fm.delete ) {
				BitSetWrapper deleteRecords = KvRowReaderFactory.getInstance().getPinnedBitSets(
					this.repository.get(this.schemaRepositoryName).fm.tableName, 
					KvRowReaderFactory.getInstance().getDeleteId( mergeId ) );
				
				if ( null != deleteRecords) {
					mixedQueryMatchedIds.getDocumentSequences().andNot(deleteRecords);
				}
			}

			return mixedQueryMatchedIds;
			
		} catch (Exception e) {
			IdSearchLog.l.fatal("Error in Searcher: could not execute " + e.getMessage(), e);
			throw new IOException("Federated Query Failure: " + whereQuery + "\n" + e.getMessage());
		}
	}

	 protected static ExecutorService ES = null;
	 private void init(){
		 if(null == ES) {
			 ES = Executors.newFixedThreadPool(30);
			 ShutdownCleanup.getInstance().addExectorService(ES);
		 }
	 }

	 /**
	  * Get all the field values for the select fields
	  * @param mergeId - Partion Key
	  * @param matchIds - Filter for the given Id Sequences
	  * @param selectFields - Selectable fields as comma separated
	  * @return - Matched document id with the object
	  * @throws IOException
	  * @throws InterruptedException
	  * @throws ExecutionException
	  */
	private final Map<String, Object> getAllSelectFieldValues(final String mergeId, 
			final BitSetOrSet matchIds, final String selectFields) throws IOException, InterruptedException, ExecutionException {
		
		String rowId = null;
		Map<String, Object> fieldWithDocIdAndValue = new HashMap<String, Object>();
		byte[] matchingIdsB = null;
		if( null != matchIds )
			matchingIdsB = SortedBytesBitset.getInstanceBitset().bitSetToBytes(matchIds.getDocumentSequences());
		
		String[] selectFieldsA = patternComma.split(selectFields);
		Map<String, Future<Map<Integer, Object>>> fieldWithfuture = new HashMap<String, Future<Map<Integer, Object>>>();
		KVDataSchema dataScheme =  repository.get(schemaRepositoryName);
		Field fld = null;
		int outputType = -1;
		String isRepeatable = null;
		String isCompressed = null;
		boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
		String rowKey = ( isEmpty) ? "" : mergeId + "_";
		
		init();
		
		for (String field : selectFieldsA) {
			rowId = rowKey + field;
			if ( ! dataScheme.fldWithDataTypeMapping.containsKey(field) ) {
				throw new IOException("Unknown Field on select clause " + field);
			}
			
			outputType = dataScheme.fldWithDataTypeMapping.get(field);
			outputType = (outputType == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : outputType;
			fld = dataScheme.fm.nameWithField.get(field);
			isRepeatable = fld.isRepeatable ? "true" : "false";
			isCompressed = fld.isCompressed ? "true" : "false";

			HSearchProcessingInstruction instruction = new HSearchProcessingInstruction(
				HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, outputType,
				isRepeatable + "\t" + isCompressed);
			
			String filterQuery = "*|*";
			
			StorageReader reader = new StorageReader(dataRepository, rowId, 
				matchIds, matchingIdsB, field, filterQuery, instruction, fld.isCachable);
			Future<Map<Integer, Object>> future = ES.submit(reader);

			fieldWithfuture.put(field, future);
		}
		Set<String> fields = fieldWithfuture.keySet();
		Future<Map<Integer, Object>> future = null;
		for (String field : fields){
			future = fieldWithfuture.get(field);
			fieldWithDocIdAndValue.put(field, future.get());
		}
		
		return fieldWithDocIdAndValue;
	}

	/**
	 * Sort on the given fields.
	 * @param sorters - Comma separated sort fields
	 * @return - Sorted Rows
	 * @throws ParseException
	 */
	public final Set<KVRowI> sort (final String... sorters) throws ParseException {
		
		GroupSorterSequencer[] sortSequencer = new GroupSorterSequencer[sorters.length];

		int index = 0;
		int fieldSeq = 0;
		int dataType = -1;
		FieldType fldType = null;
		boolean sortType = false;

		KVDataSchema dataSchema = repository.get(schemaRepositoryName); 
		
		for (String sorterName : sorters) {
			
			int sorterLen = ( null == sorterName ) ? 0 : sorterName.length();
			if ( sorterLen == 0 ) throw new ParseException("Invalid blank sorter", 0);
			
			char firstChar = sorterName.charAt(0);
			if('^' == firstChar){
				sortType = true;
				sorterName = sorterName.substring(1);
			}
			else{
				sortType = false;				
			}
			
			fieldSeq = dataSchema.nameToSeqMapping.get(sorterName);
			dataType = dataSchema.fldWithDataTypeMapping.get(sorterName);
			fldType = Datatype.getFieldType(dataType);

			GroupSorterSequencer seq = new GroupSorterSequencer(fldType,fieldSeq,index,sortType);
			
			sortSequencer[index++] = seq;
		}
		GroupSorter gs = new GroupSorter();
		
		for (GroupSorterSequencer seq : sortSequencer) {
			gs.setSorter(seq);
		}
		
		int resultsetT = this.resultset.size();
		GroupSortedObject[] sortedContainer = new GroupSortedObject[resultsetT];
		this.resultset.toArray(sortedContainer);
		gs.sort(sortedContainer);
		this.resultset = new LinkedHashSet<KVRowI>(resultsetT);
		for (int j=0; j < resultsetT; j++) {
			this.resultset.add((KVRowI)sortedContainer[j]);
		}

		return this.resultset;
	}
	
	/**
	 * Sort given data on given fields
	 * @param rows
	 * @param sorters
	 * @return
	 * @throws ParseException
	 */
	public final Set<KVRowI> sort (Set<KVRowI> rows , final String... sorters) throws ParseException 
	{
		GroupSorterSequencer[] sortSequencer = new GroupSorterSequencer[sorters.length];

		int index = 0;
		int fieldSeq = 0;
		int dataType = -1;
		FieldType fldType = null;
		boolean sortType = false;

		KVDataSchema dataSchema = repository.get(schemaRepositoryName); 
		for (String sorterName : sorters) 
		{
			int sorterLen = ( null == sorterName ) ? 0 : sorterName.length();
			if ( sorterLen == 0 ) throw new ParseException("Invalid blank sorter", 0);
			char firstChar = sorterName.charAt(0);
			if('^' == firstChar)
			{
				sortType = true;
				sorterName = sorterName.substring(1);
			}
			else
			{
				sortType = false;	
			}
			
			fieldSeq = dataSchema.nameToSeqMapping.get(sorterName);
			dataType = dataSchema.fldWithDataTypeMapping.get(sorterName);
			fldType = Datatype.getFieldType(dataType);
	
			GroupSorterSequencer seq = new GroupSorterSequencer(fldType,fieldSeq,index,sortType);
			sortSequencer[index++] = seq;
		}

		GroupSorter gs = new GroupSorter();
		for (GroupSorterSequencer seq : sortSequencer) 
		{
			gs.setSorter(seq);
		}
		
		int resultsetT = rows.size();
		GroupSortedObject[] sortedContainer = new GroupSortedObject[resultsetT];
		rows.toArray(sortedContainer);
		gs.sort(sortedContainer);
		rows = new LinkedHashSet<KVRowI>(resultsetT);
		
		for (int j=0; j < resultsetT; j++) 
		{
			rows.add((KVRowI)sortedContainer[j]);
		}

		return rows;
	}
	
	/**
	 * Get the sorted results
	 * @return Set of Records
	 */
	public final Set<KVRowI> getResult() {
		return this.resultset;
	}

	/**
	 * Get the facet counts
	 * @return - Facet field name, unique values and counts
	 */
	public final Map<String, Map<Object, FacetCount>> getFacetResult() {
		return this.facetsMap;
	}
	
	/**
	 * Get the facet counts
	 * @param matchedIds - The matched ids
	 * @param mergeId - Partion Key
	 * @param facetFields - The facetable fields 
	 * @return - The Facet count Field, Unique values and counts.
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws ParseException
	 */
	public final Map<String, Map<Object, FacetCount>> createFacetCount(
		final BitSetWrapper matchedIds, final String mergeId, final String facetFields) 
		throws NumberFormatException, IOException, ParseException {
		
		List<String> facetFieldLst = new ArrayList<String>();
		LineReaderUtil.fastSplit(facetFieldLst, facetFields, ',');

		KVDataSchema dataScheme =  repository.get(schemaRepositoryName);
		HSearchQuery hq = new HSearchQuery("*|*");
		
		/**
		 * If multi threaded facet is enabled the do multi threaded facetting
		 */
		boolean isMultiThreadedFacet = HSearchConfig.getInstance().getConfiguration().getBoolean("hsearch.facet.multithread.enabled", false);
		
		if(isMultiThreadedFacet){
			return createThreadedFacetCount(matchedIds, mergeId, facetFieldLst, dataScheme, hq);
		}

		Field fld = null;
		String isRepeatable = null;
		String isCompressed = null;
		int outputType;
		String rowId = null;

		boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
		String rowKey = ( isEmpty) ? "" : mergeId + "_";
		
		FacetProcessor facetProcessor = new FacetProcessor(matchedIds);
		for (String field : facetFieldLst) {
			
			rowId = rowKey + field;
			fld = dataScheme.fm.nameWithField.get(field);
			byte[] bytes = KvRowReaderFactory.getInstance().getReader(fld.isCachable).readRowBlob(
				dataRepository, rowId.getBytes());
			if(null == bytes) continue;
			
			isRepeatable = fld.isRepeatable ? "true" : "false";
			isCompressed = fld.isCompressed ? "true" : "false";
			outputType = dataScheme.fldWithDataTypeMapping.get(field);
			outputType = (outputType == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : outputType;

			HSearchProcessingInstruction instruction = new HSearchProcessingInstruction(
				HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, outputType,
				isRepeatable + "\t" + isCompressed);
			

			IHSearchTable table = ScalarFilter.createTable(instruction);
			Map<Object, FacetCount> facetValues = this.facetsMap.get(field);
			facetProcessor.setPreviousFacet(facetValues);
			table.get(bytes, hq, facetProcessor);
			this.facetsMap.put(field, facetProcessor.currentFacets);
		}
		return this.facetsMap;
	}		
	
	private final Map<String, Map<Object, FacetCount>> createThreadedFacetCount(final BitSetWrapper matchedIds, 
				final String mergeId, final List<String> facetFieldLst, KVDataSchema dataScheme, HSearchQuery hq) 
				throws NumberFormatException, IOException, ParseException {

		Field fld = null;
		String isRepeatable = null;
		String isCompressed = null;
		int outputType;
		String rowId = null;
		
		Map<String, Future<Map<Object, FacetCount>>> fieldWithfuture = new HashMap<String, Future<Map<Object, FacetCount>>>();
		
		boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
		String rowKey = ( isEmpty) ? "" : mergeId + "_";
		
		if(DEBUG_ENABLED)
			IdSearchLog.l.debug("Performing Multi threaded facet");
		
		/**
		 * Initialize Executor Service
		 */
		init();
		
		for (String field : facetFieldLst) {
			
			rowId = rowKey + field;
			fld = dataScheme.fm.nameWithField.get(field);
			
			isRepeatable = fld.isRepeatable ? "true" : "false";
			isCompressed = fld.isCompressed ? "true" : "false";
			outputType = dataScheme.fldWithDataTypeMapping.get(field);
			outputType = (outputType == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : outputType;

			HSearchProcessingInstruction instruction = new HSearchProcessingInstruction(
				HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, outputType,
				isRepeatable + "\t" + isCompressed);
			Map<Object, FacetCount> facetValues = this.facetsMap.get(field);
			FacetSearch facetSearch = new FacetSearch(rowId, dataRepository, hq, fld, matchedIds, instruction, facetValues);
			Future<Map<Object, FacetCount>> futureFacets = ES.submit(facetSearch);
			fieldWithfuture.put(field, futureFacets);
		}
		
		Set<String> fields = fieldWithfuture.keySet();
		Future<Map<Object, FacetCount>> future = null;
		
		for (String field : fields){
			try {
				
				future = fieldWithfuture.get(field);
				
				Map<Object, FacetCount> facetResult = future.get();
				if(null == facetResult)
					continue;
				
				this.facetsMap.put(field, facetResult);
				
			} catch (Exception e) {
				IdSearchLog.l.fatal("Error in Facetting: could not execute because " + e.getMessage(), e);
				throw new IOException("Facet Query Failure for field: " + field + "\n" + e.getMessage());
			}
		}
		
		return this.facetsMap;
		
	}
	
	/**
	 * Get a non repeated field HashMap
	 * @param mergeId	partion Id
	 * @param aFieldName	Field name
	 * @return Non repeated field value mapped to the internal Id
	 * @throws NumberFormatException
	 * @throws IOException
	 * @throws ParseException
	 */
	public final Map<Object, Integer> getANonRepeatedField(final String mergeId, 
		final BitSetWrapper matchedIds, final String aFieldName) 
		throws NumberFormatException, IOException, ParseException {

		KVDataSchema dataScheme =  repository.get(schemaRepositoryName);
		Field fld = null;
		String isRepeatable = null;
		String isCompressed = null;
		int outputType;
		
		boolean isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
		String rowId = ( isEmpty) ? aFieldName : mergeId + "_" + aFieldName;

		fld = dataScheme.fm.nameWithField.get(aFieldName);
		byte[] bytes = KvRowReaderFactory.getInstance().getReader(fld.isCachable).readRowBlob(
			dataRepository, rowId.getBytes());
		//if(null == bytes) return null;
		
		isRepeatable = fld.isRepeatable ? "true" : "false";
		isCompressed = fld.isCompressed ? "true" : "false";
		outputType = dataScheme.fldWithDataTypeMapping.get(aFieldName);
		outputType = (outputType == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : outputType;

			
		HSearchProcessingInstruction instruction = new HSearchProcessingInstruction(
			HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, outputType,
			isRepeatable + "\t" + isCompressed);

		IHSearchTable table = ScalarFilter.createTable(instruction);
		final Map<Object, Integer> foundValues = new HashMap<Object, Integer>();
		
    	table.get(bytes, new HSearchQuery("*|*"), new MapperKVBaseEmptyImpl() {
			
			@Override
			public final boolean onRowCols(final int key, final Object value) {
				if (null != matchedIds) {
					if ( ! matchedIds.get(key)) return false;
				}
				
				foundValues.put(value, key);
				return false;
			}

			@Override
			public boolean onRowCols(BitSetWrapper ids, Object value) {
				return false;
			}
		});
			
		return foundValues;
	}
	


	/**
	 * Cleans up the searcher and makes it ready for the next search, Reuse
	 */
	public final void clear() {
		if(null != this.resultset)this.resultset.clear();
		if(null != this.facetsMap)this.facetsMap.clear();
		if(null != this.pivotFacetsMap)this.pivotFacetsMap.clear();
	}
	
	
	/**
	 * Federated Search
	 * @return
	 */
	private FederatedSearch createFederatedSearch() {
		
		FederatedSearch ff = new FederatedSearch() {

			@Override
			public BitSetOrSet populate(final String type, final String queryId,
					final String rowId, final Map<String, Object> filterValues) throws IOException {
				
				String totalQuery = filterValues.get("query").toString();
				String fieldQuery = filterValues.get("fieldQuery").toString();
				String field = filterValues.get("fieldName").toString();
				
				int tableNameLen = ( null == dataRepository) ? 0 : dataRepository.trim().length();
				
				if ( 0 == tableNameLen) {
					IdSearchLog.l.fatal("Unknown data repository for query " + queryId);
					throw new IOException("Unknown data repository for query " + queryId);
				}
				
				KVDataSchema dataScheme = repository.get(schemaRepositoryName);
				if ( null == field) {
					String msg = "Field can not be null - " + rowId + "\n" + (dataScheme.fldWithDataTypeMapping + "\t" + field);
					throw new IOException(msg);
				}
				
				if ( ! dataScheme.fldWithDataTypeMapping.containsKey(field)) {
					throw new IOException("Field Is not mapped properly - " + field);
				}
				int outputType = dataScheme.fldWithDataTypeMapping.get(field);
				
				Field fld = dataScheme.fm.nameWithField.get(field);
				String isRepeatable = fld.isRepeatable ? "true" : "false";
				String isCompressed = fld.isCompressed ? "true" : "false";

				boolean isTextSearch = fld.isDocIndex;
				
				HSearchProcessingInstruction instruction = new HSearchProcessingInstruction(HSearchProcessingInstruction.PLUGIN_CALLBACK_ID, 
					outputType, isRepeatable + "\t" + isCompressed);
								
				StorageReader reader = new StorageReader(dataRepository, rowId, totalQuery, 
														instruction, fld.isCachable);
				
				BitSetOrSet rows = null;
				if ( isTextSearch ) 
					rows = reader.readStorageTextIds(fld, fieldQuery, checkForAllWords, 
								field, isNGramEnabled, checkExactPhrase);
				else 
					rows = reader.readStorageIds();
				
				return rows;
			}
		};
		return ff;
	}
	
	/**
	 * Query parsing - Where clause
	 * @param mergeId - Partioned Id
	 * @param whereQuery - Where query
	 * @param queryDetails - Parsed output
	 * @return - Where Query
	 * @throws IOException
	 */
	private String parseWhereQuery(final String mergeId, String whereQuery,
			Map<String, QueryPart> queryDetails) throws IOException {
		String skeletonQuery = patternExtraWhitespaces.matcher(whereQuery).replaceAll(" ");
		skeletonQuery = patternBracketsOutsideQuotes.matcher(whereQuery).replaceAll("");
		String[] splittedQueries = patternBooleans.split(skeletonQuery);
		
		int index = -1;
		int colonIndex = -1;
		int totalQueries = 0;
		String fieldName = "";
		String fieldQuery = "";
		String totalQuery = "";
		String rowKey = "";
		boolean isEmpty = false;

		for (String splittedQuery : splittedQueries) {
			splittedQuery = splittedQuery.trim();
			index = whereQuery.indexOf(splittedQuery);
			String queryId = "q" + totalQueries++; 
			whereQuery = whereQuery.substring(0, index) + queryId +  whereQuery.substring(index + splittedQuery.length());
			colonIndex = splittedQuery.indexOf(':');
			if ( -1 == colonIndex) {
				throw new IOException("Invalid query fyntax. Expecting FIELD:VALUE and not " + splittedQuery);
			}
			fieldName = splittedQuery.substring(0,colonIndex);
			fieldQuery = splittedQuery.substring(colonIndex + 1,splittedQuery.length());
			totalQuery = "*|" + fieldQuery;
    		isEmpty = ( null == mergeId) ? true : (mergeId.length() == 0);
    		rowKey = ( isEmpty) ? fieldName : mergeId + "_" + fieldName;
			QueryPart qpart = new QueryPart(rowKey);

			qpart.setParam("query", totalQuery);
			qpart.setParam("allwords", this.checkForAllWords);
			qpart.setParam("fieldName", fieldName);
			qpart.setParam("fieldQuery", fieldQuery);
			queryDetails.put(queryId, qpart);
		}
		return whereQuery;
	}


	/**
	 * Pivot faceting
	 * @param mergeId - Partioned Id
	 * @param pivotFacetFields - Pivot Fields
	 * @param facetQuery - Facet Query
	 * @param aBlankrow 
	 * @return Pivot Facets with internal facets
	 * @throws Exception
	 */
	public final Map<String, List<HsearchFacet>> pivotFacet(final String mergeId, final String pivotFacetFields, 
		final String facetQuery, final KVRowI aBlankrow) throws Exception{
		String[] pivotFacets = pivotFacetFields.split("\\|");
		this.pivotFacetsMap = new HashMap<String, List<HsearchFacet>>();
		IEnricher enricher = null;
		for (String aPivotFacet : pivotFacets) {
			String[] fields = patternComma.split(aPivotFacet);
			search(mergeId, aPivotFacet, facetQuery, aBlankrow, enricher);
			Set<KVRowI> result = getResult();
			HsearchFacet root = new HsearchFacet("root", new TypedObject("root"), new ArrayList<HsearchFacet>());
			HsearchFacet current = root;
			for (KVRowI kvRowI : result) {
				for (String aField : fields) {
					current = current.getChild(aField, kvRowI.getValueNative(aField));				
				}
		        current = root;			
			}
			this.pivotFacetsMap.put(aPivotFacet, root.getinternalFacets());
			this.resultset.clear();
		}
		
		return this.pivotFacetsMap;
	}
	
}