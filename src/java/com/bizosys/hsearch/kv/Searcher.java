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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.FederatedSearch;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;
import com.bizosys.hsearch.functions.GroupSorter;
import com.bizosys.hsearch.functions.GroupSorter.GroupSorterSequencer;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.kv.impl.Datatype;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.IEnricher;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.KVDocIndexer;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;

public class Searcher {
	private static ExecutorService ES = null;

	public String dataRepository = "";
	public String schemaRepositoryName = "";
	
	public static final Pattern patternExtraWhitespaces = Pattern.compile("\\s+");
	public static final Pattern patternBooleans = Pattern.compile("( AND | OR | NOT )");
	public static final Pattern patternComma = Pattern.compile(",");
	public static final Pattern patternBracketsOutsideQuotes = Pattern.compile("(\\(|\\))(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
	public static final Pattern patternRemoveQuotes = Pattern.compile("\"");
	
	private Set<KVRowI> resultset = null;
	private Map<String, String> filters = null;
	public KVDataSchemaRepository repository = KVDataSchemaRepository.getInstance();
	public KVDocIndexer indexer = new KVDocIndexer();
	public Analyzer analyzer = null;

	private Map<String, Set<Object>> facetsMap = null;
	private Map<String, List<HsearchFacet>> pivotFacetsMap = null;
	
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	private Searcher(){
	}
	
	public Searcher(final String schemaName, final FieldMapping fm, final Analyzer analyzer){
		this.schemaRepositoryName = schemaName;
		this.repository.add(schemaRepositoryName, fm);
		this.resultset = new HashSet<KVRowI>();
		this.filters = new HashMap<String, String>();
		this.analyzer = analyzer;
		facetsMap = new HashMap<String, Set<Object>>();
		pivotFacetsMap = new HashMap<String, List<HsearchFacet>>();
	}

	public final Set<String> searchRegex(final String dataRepository,
			final String mergeIdPattern, final String selectQuery, String whereQuery,
			KVRowI blankRow, IEnricher... enrichers) throws IOException, InterruptedException, ExecutionException, FederatedSearchException  {

		long start = System.currentTimeMillis();
		List<String> rowIds = HReader.getMatchingRowIds(dataRepository, mergeIdPattern);
		if ( null == rowIds) return null;
		if ( rowIds.size() == 0 ) return null;
		
		Set<String> mergeIds = new HashSet<String>();
		int lastIndex = -1;
		for (String mergeIdWithFieldId : rowIds) {
			lastIndex = mergeIdWithFieldId.lastIndexOf('_');
			mergeIds.add(  mergeIdWithFieldId.substring(0, lastIndex) );
		}

		if ( DEBUG_ENABLED) {
			long end = System.currentTimeMillis();
			int mergeIdsT = ( null == mergeIds) ? 0 : mergeIds.size();
			HSearchLog.l.debug( mergeIdPattern +  " rows regex matched total = " + mergeIdsT + " in Time ms, " + (end - start));
			start = end;
		}
		
		for (String mergeId : mergeIds) {
			search(dataRepository, mergeId, selectQuery, whereQuery, blankRow, enrichers);
		}

		if ( DEBUG_ENABLED) {
			long end = System.currentTimeMillis();
			HSearchLog.l.debug( mergeIdPattern +  " fields retrieved in Time ms, " + (end - start));
		}

		return mergeIds;
	}
	
	@SuppressWarnings("unchecked")
	public final void search(final String dataRepository, 
			final String mergeId, String selectQuery, String whereQuery, 
			KVRowI blankRow, IEnricher... enrichers) throws IOException, InterruptedException, ExecutionException, FederatedSearchException {

		BitSetOrSet foundIds = null;
		Map<String, Object> foundResults = null;
		BitSetOrSet foundValues = null;
		Set<Integer> documentIds = null;
		Set<String> fields = null;
		Map<Integer, Object> result = null;
		Map<Integer,KVRowI> mergedResult = new HashMap<Integer, KVRowI>();

		this.dataRepository = dataRepository;
		boolean isWhereQueryEmpty = ( null == whereQuery) ? true : (whereQuery.length() == 0);
		if(!isWhereQueryEmpty)
			foundIds = getIds(mergeId, whereQuery);

		boolean isSelectQueryEmpty = ( null == selectQuery) ? true : (selectQuery.length() == 0);
		if(!isSelectQueryEmpty)
			foundResults = getValues(mergeId, selectQuery);
		
		if(null == foundResults)
			return;
		
		fields = foundResults.keySet();
		for (String field : fields) {

			result = (Map<Integer, Object>) foundResults.get(field);
			documentIds = new HashSet<Integer>(result.keySet());
			
			//if where query is not empty do the intersection
			if(!isWhereQueryEmpty){
				foundValues = new BitSetOrSet();
				foundValues.setDocumentIds(documentIds);
				foundValues.and(foundIds);
				documentIds = foundValues.getDocumentIds();
			}

			for (Integer id : documentIds) {
				if (mergedResult.containsKey(id)){
					KVRowI aRow = mergedResult.get(id);
					aRow.setValue(field, result.get(id));
				}
				else {
					KVRowI aRow = blankRow.create(repository.get(schemaRepositoryName));
					aRow.setValue(field, result.get(id));
					aRow.setId(id);
					aRow.setmergeId(mergeId);
					mergedResult.put(id, aRow);
				}
			}
		}

		resultset.addAll(mergedResult.values());
		
		if ( null != enrichers) {
			for (IEnricher enricher : enrichers) {
				if ( null != enricher) enricher.enrich(this.resultset);
			}
		}
	}

	private final BitSetOrSet getIds(final String mergeId, String whereQuery) throws IOException {

		try {
	
			String skeletonQuery = patternExtraWhitespaces.matcher(whereQuery).replaceAll(" ");
			skeletonQuery = patternBracketsOutsideQuotes.matcher(whereQuery).replaceAll("");
			String[] splittedQueries = patternBooleans.split(skeletonQuery);
			
			int index = -1;
			int colonIndex = -1;
			int totalQueries = 0;
			String fieldName = "";
			String fieldQuery = "";
			Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
	
			for (String splittedQuery : splittedQueries) {
				splittedQuery = splittedQuery.trim();
				index = whereQuery.indexOf(splittedQuery);
				String queryId = "q" + totalQueries++; 
				whereQuery = whereQuery.substring(0, index) + queryId +  whereQuery.substring(index + splittedQuery.length());
				colonIndex = splittedQuery.indexOf(':');
				fieldName = splittedQuery.substring(0,colonIndex);
				fieldQuery = "*|" + splittedQuery.substring(colonIndex + 1,splittedQuery.length());
				filters.put(fieldName, fieldQuery);
				QueryPart qpart = new QueryPart(mergeId + "_" + fieldName);
				qpart.setParam("query", fieldQuery);
				queryDetails.put(queryId, qpart);
			}
			
			FederatedSearch ff = createFederatedSearch();
			BitSetOrSet mixedQueryMatchedIds = ff.execute(whereQuery, queryDetails);
			return mixedQueryMatchedIds;
			
		} catch (Exception e) {
			HSearchLog.l.fatal("Error in Searcher: could not execute " + e.getMessage(), e);
			throw new IOException("Federated Query Failure: " + whereQuery + "\n" + e.getMessage());
		}
	}

	 private void init(){
		 if(null == ES) ES = Executors.newFixedThreadPool(20);
	 }

	private final Map<String, Object> getValues(final String mergeId, final String selectFields) throws IOException, InterruptedException, ExecutionException {
		
		String filterQuery = null;
		String rowId = null;
		Map<String, Object> individualResults = new HashMap<String, Object>();

		String[] selectFieldsA = patternComma.split(selectFields);
		Map<String, Future<Object>> fieldWithfuture = new HashMap<String, Future<Object>>();
		init();
		for (String field : selectFieldsA) {
			filterQuery = filters.get(field);
			if(null == filterQuery)
				filterQuery = "*|*";
			
			rowId = mergeId + "_" + field;
			StorageReader reader = new StorageReader(repository, schemaRepositoryName, indexer, analyzer, dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS);
			Future<Object> future = ES.submit(reader);
			fieldWithfuture.put(field, future);
		}
		Set<String> fields = fieldWithfuture.keySet();
		Future<Object> future = null;
		for (String field : fields){
			future = fieldWithfuture.get(field);
			individualResults.put(field, future.get());
		}
		
		return individualResults;
	}
	
	@SuppressWarnings("unchecked")
	private final Set<Object> getValues(final String mergeId, final String selectFields,  final String foundIds, final Map<String, Object> individualResults)
		throws IOException, InterruptedException, ExecutionException {
		
		String filterQuery = null;
		String rowId = null;
		BitSetOrSet destination = new BitSetOrSet();

		String[] selectFieldsA = patternComma.split(selectFields);
		boolean isFirst = true;

		try {
			for (String field : selectFieldsA) {
				
				filterQuery = foundIds + "|*";
				rowId = mergeId + "_" + field;
				
				StorageReader reader = new StorageReader(repository, schemaRepositoryName, indexer, 
					analyzer, dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS);
				
				Map<Integer, Object> readingIdWithValue = (Map<Integer, Object>) reader.readStorage() ;
				if ( DEBUG_ENABLED ) {
					int readingIdWithValueLen = ( null == readingIdWithValue) ? 0 : readingIdWithValue.size();
					if(0 == readingIdWithValueLen) HSearchLog.l.debug("No data fetched for " + field);
				}

				individualResults.put(field, readingIdWithValue);
				Set<Integer> readingId = new HashSet<Integer>(readingIdWithValue.keySet());

				if(isFirst) {
					isFirst = false;
					destination.setDocumentIds(readingId);
					continue;
				}
				
				BitSetOrSet source = new BitSetOrSet();
				source.setDocumentIds(readingId);
				destination.and(source);
			}
			return destination.getDocumentIds();

		} catch (FederatedSearchException e) {
			HSearchLog.l.fatal("Federated Search Exception", e);
			throw new IOException(e);
		}
	}	

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
			dataType = dataSchema.dataTypeMapping.get(sorterName);
			fldType = Datatype.getFieldType(dataType);

			GroupSorterSequencer seq = new GroupSorterSequencer(fldType,fieldSeq,index,sortType);
			
			sortSequencer[index++] = seq;
		}
		GroupSorter gs = new GroupSorter();
		
		for (GroupSorterSequencer seq : sortSequencer) {
			gs.setSorter(seq);
		}
		int resultsetT = resultset.size();
		GroupSortedObject[] sortedContainer = new GroupSortedObject[resultsetT];
		resultset.toArray(sortedContainer);
		gs.sort(sortedContainer);
		resultset = new HashSet<KVRowI>();
		for (int j=0; j < resultsetT; j++) {
			resultset.add((KVRowI)sortedContainer[j]);
		}

		return this.resultset;
	}
	
	public final Set<KVRowI> getResult() {
		return this.resultset;
	}

	@SuppressWarnings("unchecked")
	public final Map<String, Set<Object>> facet(final String dataRepository, final String mergeId, final String facetQuery, final String facetFields, final String facetSort, final KVRowI combiner) throws IOException, ParseException, InterruptedException, ExecutionException{
		Map<String, Object> individualResults = new HashMap<String, Object>(); 
		BitSetOrSet foundIds = getIds(mergeId, facetQuery);
		this.dataRepository = dataRepository;
		getValues(mergeId, facetFields, foundIds.toString(), individualResults);
		
		for (String field : individualResults.keySet()) {
			Map<Integer, Object> res = (Map<Integer, Object>) individualResults.get(field);
			Set<Object> facets = new TreeSet<Object>(); 
			for (Integer key : res.keySet()) {
				facets.add(res.get(key));
			}
			facetsMap.put(field, facets);
		}
		
		return facetsMap;
	}
	
	public final Map<String, List<HsearchFacet>> pivotFacet(final String dataRepository, final String mergeId, final String facetQuery, final String pivotFacetFields, final String pivotFacetSort, final KVRowI combiner) throws IOException, ParseException, InterruptedException, ExecutionException, FederatedSearchException{
		String[] pivotFacets = pivotFacetFields.split("\\|");
		IEnricher enricher = null;
		for (String aPivotFacet : pivotFacets) {
			String[] fields = patternComma.split(aPivotFacet);
			search(dataRepository, mergeId, aPivotFacet, facetQuery, combiner, enricher);
			String[] sorters = patternComma.split(pivotFacetSort);
			Set<KVRowI> result = sort(sorters);
			HsearchFacet root = new HsearchFacet("root", new TypedObject("root"), new ArrayList<HsearchFacet>());
			HsearchFacet current = root;
			for (KVRowI kvRowI : result) {
				for (String aField : fields) {
					current = current.getChild(aField, kvRowI.getValueNative(aField));				
				}
		        current = root;			
			}
			this.pivotFacetsMap.put(aPivotFacet, root.getinternalFacets());
		}
		
		return pivotFacetsMap;
	}

	public final void clear() {
		if(null != resultset)resultset.clear();
		if(null != filters)filters.clear();
		if(null != ES)ES.shutdown();
		if(null != facetsMap)facetsMap.clear();
		if(null != pivotFacetsMap)pivotFacetsMap.clear();
	}
	
	private final Searcher cloneShallow() {
		Searcher searcher = new Searcher();
		searcher.schemaRepositoryName = this.schemaRepositoryName;
		return searcher;
	}
	
	private FederatedSearch createFederatedSearch() {
		
		FederatedSearch ff = new FederatedSearch(2) {

			@SuppressWarnings("unchecked")
			@Override
			public BitSetOrSet populate(String type, String queryId,
					String rowId, Map<String, Object> filterValues) throws IOException {
				
				String filterQuery = filterValues.values().iterator().next().toString();
				StorageReader reader = new StorageReader(repository, schemaRepositoryName, indexer, analyzer, dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_ID);
				Set<Integer> readingIds = (Set<Integer>) reader.readStorage();
				BitSetOrSet rows = new BitSetOrSet();
				rows.setDocumentIds(readingIds);
				return rows;
			}
		};
		return ff;
	}
	
	public final void setFieldTypeCodes(final Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public final void setDocumentTypeCodes(final Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}
}