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

import java.util.Map;
import java.util.concurrent.Callable;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.KvRowReaderFactory;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

/**
 * 
 * FacetSearch is used to get a facet count of a given field.
 *
 */
public class FacetSearch implements Callable<Map<Object, FacetCount>> {

	String rowId = null;
	String dataRepository = null;
	Field fld = null;
	BitSetWrapper matchedIds = null;
	HSearchProcessingInstruction instruction = null;
	HSearchQuery hq = null;
	Map<Object, FacetCount> facetValues = null;
	
	/**
	 * 
	 * @param rowId
	 * @param dataRepository
	 * @param hq
	 * @param fld
	 * @param matchedIds
	 * @param instruction
	 */
	public FacetSearch(String rowId, String dataRepository, HSearchQuery hq, Field fld, BitSetWrapper matchedIds, 
			HSearchProcessingInstruction instruction, Map<Object, FacetCount> facetValues) {
		
		this.rowId = rowId;
		this.dataRepository = dataRepository;
		this.hq = hq;
		this.fld = fld;
		this.matchedIds = matchedIds;
		this.instruction = instruction;
		this.facetValues = facetValues;
	}
	
	/**
	 * For a given field reads the row blob and calculates the facet value.
	 * @return This returns a Map of value and a facet count.
	 */
	@Override
	public Map<Object, FacetCount> call() throws Exception {
		
		byte[] bytes = KvRowReaderFactory.getInstance().getReader(fld.isCachable).readRowBlob(
				dataRepository, rowId.getBytes());
			if(null == bytes) return null;

		IHSearchTable table = ScalarFilter.createTable(instruction);
		FacetProcessor facetProcessor = new FacetProcessor(matchedIds);
		facetProcessor.setPreviousFacet(facetValues);
		table.get(bytes, hq, facetProcessor);
		return facetProcessor.currentFacets;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return sb.append("Datarepo : ").append(dataRepository).append("\tRow ID : ").append(rowId).append("\t instruction : ")
				.append(instruction).append("\t Field : ").append(fld.name).toString();
	}
}
