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
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.QueryPart;

/**
 * Way to influence and tap into the search processing flow.
 * @author abinash
 *
 */
public interface ISearcherPlugin {
	
	/**
	 * Helps in joining with external data sources at bitset labels.
	 * Calls after joining is performed.
	 * @param mergeId - Partion Id
	 * @param foundIds - Found Ids, The bitsets
	 * @param whereParts - The where fields 
	 * @param foundIdWithValueObjects -This is a clear object. Searcher does not use it for any purpose.
	 */
	void onJoin(String mergeId, BitSetWrapper foundIds, 
		Map<String, QueryPart> whereParts, Map<Integer,KVRowI> foundIdWithValueObjects) throws IOException ;
	
	/**
	 * Calls after the facet is computed. Any modification can be done after this.
	 * @param mergeId
	 * @param facets
	 */
	void onFacets(String mergeId, Map<String, Map<Object, FacetCount>> facets) throws IOException;

	/**
	 * Invokes before the select happenss
	 * @param mergeId
	 * @param foundIds
	 */
	void beforeSelect(String mergeId, BitSetWrapper foundIds) throws IOException;
	
	/**
	 * Calls after the selection is over.
	 * @param mergeId
	 * @param foundIds
	 */
	void afterSelect(String mergeId, BitSetWrapper foundIds, Set<KVRowI> resultset) throws IOException;

	/**
	 * Calls before selection happens on a sorted fields
	 * @param mergeId
	 * @param foundIds
	 */
	void beforeSelectOnSorted(String mergeId, BitSetWrapper foundIds) throws IOException;

	/**
	 * Invokes after the selection is done on sorted fields 
	 * @param mergeId
	 * @param foundIds
	 */
	void afterSelectOnSorted(String mergeId, BitSetWrapper foundIds) throws IOException;

	/**
	 * Before the sort is performed. This is a place to influence the ranking also
	 * Internal ranks are provided which are nothing but the phrase mapping.
	 * @param mergeId
	 * @param sortFields
	 * @param resultSet Result set
	 * @param defaultRankedIds Sorted Ids based on phrases
	 * @return
	 */
	boolean beforeSort(String mergeId, String sortFields, Set<KVRowI> resultSet, Map<Integer, BitSetWrapper> rankBuckets) throws IOException;

	/**
	 * After the sort is over.
	 * @param mergeId
	 * @param resultSet
	 */
	void afterSort(String mergeId, Set<KVRowI> resultSet) throws IOException;
	
}
