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
import java.util.regex.Pattern;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.FederatedSearch;
import com.bizosys.hsearch.federate.FederatedSearchException;
import com.bizosys.hsearch.federate.QueryPart;
import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;
import com.bizosys.hsearch.functions.GroupSorter;
import com.bizosys.hsearch.functions.GroupSorter.GroupSorterSequencer;
import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.IEnricher;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.KVDocIndexer;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;

public class Searcher {

	public String dataRepository = "";
	String schemaRepositoryName = "";
	
	public static final Pattern patternWhitespaceStart = Pattern.compile("\\s+");
	public static final Pattern patternBracketsStart = Pattern.compile("\\(");
	public static final Pattern patternBracketsEnd = Pattern.compile("\\)");
	public static final Pattern patternBooleans = Pattern.compile("( AND | OR | NOT )");
	public static final Pattern patternComma = Pattern.compile(",");
	
	List<KVRowI> resultset = new ArrayList<KVRowI>();
	KVDataSchemaRepository repository = KVDataSchemaRepository.getInstance();
	
	KVDocIndexer indexer = new KVDocIndexer();
	
	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();
	
	
	private Searcher(){
	}
	
	public Searcher(final String schemaName, final FieldMapping fm){
		this.schemaRepositoryName = schemaName;
		repository.add(schemaRepositoryName, fm);
	}

	public Set<String> searchRegex(final String dataRepository,
			final String mergeIdPattern, String selectQuery, String whereQuery,
			KVRowI blankRow, IEnricher... enrichers) throws IOException  {

		List<String> rowIds = HReader.getMatchingRowIds(dataRepository, mergeIdPattern);
		if ( null == rowIds) return null;
		if ( rowIds.size() == 0 ) return null;
		
		Set<String> mergeIds = new HashSet<String>();
		for (String mergeIdWithFieldId : rowIds) {
			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Analyzing rowId :" + mergeIdWithFieldId);
			int lastIndex = mergeIdWithFieldId.lastIndexOf('_');
			mergeIds.add(  mergeIdWithFieldId.substring(0, lastIndex) );
		}
		
		for (String mergeId : mergeIds) {
			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Searching in mergeId :" + mergeId);
			search(dataRepository, mergeId, selectQuery, whereQuery, blankRow, enrichers);
		}
		
		return mergeIds;
	}
	
	@SuppressWarnings("unchecked")
	public void search(final String dataRepository, 
			final String mergeId, String selectQuery, String whereQuery, 
			KVRowI blankRow, IEnricher... enrichers) throws IOException {

		StringBuilder foundIds = null;

		boolean isEmpty = ( null == whereQuery) ? true : (whereQuery.length() == 0);
		
		if(isEmpty) {
		
			foundIds = new StringBuilder("*");
		
		} else {
			
			String skeletonQuery = patternWhitespaceStart.matcher(whereQuery).replaceAll(" ");
			skeletonQuery = patternBracketsStart.matcher(skeletonQuery).replaceAll("");
			skeletonQuery = patternBracketsEnd.matcher(skeletonQuery).replaceAll("");
			String[] splittedQueries = patternBooleans.split(skeletonQuery);
			
			int index = -1;
			int colonIndex = -1;
			int totalQueries = 0;
			String fieldName = "";
			String fieldText = "";
			Map<String, QueryPart> queryDetails = new HashMap<String, QueryPart>();
			
			for (String splittedQuery : splittedQueries) {
				splittedQuery = splittedQuery.trim();
				index = whereQuery.indexOf(splittedQuery);
				String queryId = "q" + totalQueries++; 
				whereQuery = whereQuery.substring(0, index) + queryId +  whereQuery.substring(index + splittedQuery.length());
				colonIndex = splittedQuery.indexOf(':');
				fieldName = splittedQuery.substring(0,colonIndex);
				fieldText = splittedQuery.substring(colonIndex + 1,splittedQuery.length());
				QueryPart qpart = new QueryPart(mergeId + "_" + fieldName);
				qpart.setParam("query", "*|" + fieldText);
				queryDetails.put(queryId, qpart);
			}
			
			this.dataRepository = dataRepository;
			FederatedSearch ff = createFederatedSearch();

			//get ids first
			BitSetOrSet mixedQueryMatchedIds = null;
			try {
				mixedQueryMatchedIds = ff.execute(whereQuery, queryDetails);
			} catch (Exception e) {
				HSearchLog.l.fatal("Error in Searcher: could not execute " + e.getMessage(), e);
				throw new IOException("Federated Query Failure: " + whereQuery + "\n" + e.getMessage());
			}
			
			if(null == mixedQueryMatchedIds) return;
			Set<Object> documentIds = mixedQueryMatchedIds.getDocumentIds();
			if ( null == documentIds) return;
			if (documentIds.size() == 0 ) return;

			if ( DEBUG_ENABLED ) {
				if ( null != documentIds) 
					HSearchLog.l.debug("Matching ids " + documentIds.toString());
			}
			
			boolean firstTime = true;
			for (Object matchedId : documentIds) {
				if ( firstTime ) {
					firstTime = false;
					foundIds = new StringBuilder("{");
					foundIds.append(matchedId.toString());
				} else {
					foundIds.append(',').append(matchedId.toString());
				}
			}
			
			if(null != foundIds) foundIds.append('}');
		}
		
		//get all the values based on ids
		String filterQuery = null;
		String rowId = null;
		Map<String, Object> individualResults = new HashMap<String, Object>();
		BitSetOrSet destination = new BitSetOrSet();

		String[] selectFields = patternComma.split(selectQuery);
		boolean isFirst = true;


		try {
			for (String field : selectFields) {
				
				filterQuery = foundIds.toString() + "|*";
				rowId = mergeId + "_" + field;
				
				Map<Integer, Object> readingIdWithValue = (Map<Integer, Object>) readStorage(dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS);
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

		} catch (FederatedSearchException e) {
			HSearchLog.l.fatal("Federated Search Exception", e);
			e.printStackTrace(System.err);
		}	
		
		Set<Object> documentIds = destination.getDocumentIds();
		if ( null == documentIds) return;
		if (documentIds.size() == 0 ) return;
		
		if ( DEBUG_ENABLED ) {
			HSearchLog.l.debug("Final matching ids" + documentIds.toString());
		}
		Map<Integer,KVRowI> mergedResult = new HashMap<Integer, KVRowI>();

		for (String field : individualResults.keySet()) {
			Map<Integer, Object> res = (Map<Integer, Object>) individualResults.get(field);
			
			for (Object key : documentIds) {
				Integer id = (Integer)key;
				if (mergedResult.containsKey(id)){
					KVRowI aRow = mergedResult.get(id);
					aRow.setValue(field, res.get(id));
				}
				else {
					KVRowI aRow = blankRow.create(repository.get(schemaRepositoryName));
					aRow.setValue(field, res.get(id));
					aRow.setId((Integer)id);
					mergedResult.put((Integer)id, aRow);
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

	public List<KVRowI> sort (String... sorters) throws ParseException {
	
		GroupSorterSequencer[] sortSequencer = new GroupSorterSequencer[sorters.length];

		int index = 0;
		int fieldSeq = 0;
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
			fldType = dataSchema.dataTypeMapping.get(sorterName);
			GroupSorterSequencer seq = new GroupSorterSequencer(fldType,fieldSeq,index,sortType);
			
			sortSequencer[index++] = seq;
		}
		GroupSorter gs = new GroupSorter();
		
		for (GroupSorterSequencer seq : sortSequencer) {
			gs.setSorter(seq);
		}
		
		GroupSortedObject[] sortedContainer = new GroupSortedObject[resultset.size()];
		resultset.toArray(sortedContainer);
		
		gs.sort(sortedContainer);

		ListIterator<KVRowI> i = resultset.listIterator();
		for (int j=0; j<sortedContainer.length; j++) {
		    i.next();
		    i.set((KVRowI)sortedContainer[j]);
		}

		return this.resultset;
	}
	
	public List<KVRowI> getResult() {
		return this.resultset;
	}
	
	public void clear() {
		resultset.clear();
	}
	
	private Searcher cloneShallow() {
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
				
				Searcher s = cloneShallow();
				String filterQuery = filterValues.values().iterator().next().toString();
				Set<Integer> readingIds = (Set<Integer>) s.readStorage(dataRepository, rowId, filterQuery, HSearchProcessingInstruction.PLUGIN_CALLBACK_ID);
				BitSetOrSet rows = new BitSetOrSet();
				rows.setDocumentIds(readingIds);
				return rows;
			}
		};
		return ff;
	}
	
	public final Object readStorage(final String tableName, String rowId, String filter, final int callBackType) throws IOException {
		
		byte[] data = null;
		try {
			
			String fieldName = rowId.substring(rowId.lastIndexOf("_") + 1,rowId.length());
			KVDataSchema dataScheme = repository.get(schemaRepositoryName);
			Field fld = dataScheme.fm.nameSeqs.get(fieldName);
			int outputType = dataScheme.dataTypeMapping.get(fieldName).ordinal();
			if(callBackType == HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS){
				//if the field is not saved it canot be fetched 
				if ( !fld.isSave ) {
					HSearchLog.l.fatal("Field: " + fieldName + " is not saved cannot be selected ");
					throw new IOException("Field: " + fieldName + " is not saved cannot be selected ");
				}

				ComputeKV compute = null;	
				compute = new ComputeKV();
				
				//if docIndex is searched for value then set output type to string
				if(fld.isDocIndex)
					compute.kvType = GroupSortedObject.FieldType.STRING.ordinal();
				else
					compute.kvType = outputType;
				
				compute.rowContainer = new HashMap<Integer, Object>();

				data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, compute.kvType);

				boolean isEmpty = ( null == data) ? true : (data.length == 0);  
				if(isEmpty) return new HashMap<Integer,Object>(0);
				compute.put(data);
				
				return compute.rowContainer;
				
			} else if(callBackType == HSearchProcessingInstruction.PLUGIN_CALLBACK_ID){
				
				//change filterQuery for documents search
				if(fld.isDocIndex){
					int queryPartLoc = filter.lastIndexOf('|');
					String query = ( queryPartLoc < 0 ) ? filter : filter.substring(queryPartLoc+1);

					String docType = "DOC";
					String fieldType = fieldName;
					
					int fieldTypeLoc = fieldName.indexOf('/');
					if ( fieldTypeLoc > 0 ) {
						docType = fieldName.substring(0, fieldTypeLoc);
						fieldType = fieldName.substring(fieldTypeLoc+1);
					}
					
	    			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Query :[" + query + "] : DocType :[" + docType + "]  : Field Type:[" + fieldType + "]");

	    			Map<String, Integer> dTypes = new HashMap<String, Integer>(1);
	    			dTypes.put(docType, 1);
	    	    	setDocumentTypeCodes(dTypes);
	    			
	    			Map<String, Integer> fTypes= new HashMap<String, Integer>(1);
	    			fTypes.put(fieldType, 1);
	    	    	setFieldTypeCodes(fTypes);
	    	    	
	    			filter = indexer.parseQuery(new StandardAnalyzer(Version.LUCENE_36), docType, fieldType, query);
	    			rowId = rowId + "_I";
	    			
	    			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Document Search => rowId:[" + rowId + "] : Query :[" + filter + "]");
	    		}				
				data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, outputType);
				boolean isEmpty = ( null == data) ? true : (data.length == 0);  
				if(isEmpty) return new HashSet<Integer>(0);

				if ( DEBUG_ENABLED ){
					if(isEmpty)
					HSearchLog.l.debug("Empty data returned for rowid: " + rowId + " query: " + filter);				
				}
				
				for (byte[] dataChunk : SortedBytesArray.getInstanceArr().parse(data).values()) {
					Set<Integer> ids = new HashSet<Integer>();
					SortedBytesInteger.getInstance().parse(dataChunk).values(ids);
					return ids;
				}
			}
			return null;
		} catch (Exception e) {
			HSearchLog.l.fatal("ReadStorage Exception " + e.getMessage(), e );
			throw new IOException("ReadStorage Exception " + e.getMessage(), e);
		}
	}

	public void setFieldTypeCodes(Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public void setDocumentTypeCodes(Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}
}