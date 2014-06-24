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
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmptyImpl;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

/**
 * The bitSetRow class deserializes the hsearch index blob and filters the
 * values based on filterquery and returns a Map of key and value.
 * 
 */
public final class BitSetRow {

	/**
	 * 
	 * @param inputBytes
	 * @param filterQuery
	 * @param inputMapperInstructions
	 * @return returns a Map of key and value
	 * @throws IOException
	 */
	public static final Map<Integer, Object> process(final byte[] inputBytes, 
			final String  filterQuery,final HSearchProcessingInstruction inputMapperInstructions) throws IOException{
		return process(inputBytes, null, filterQuery, inputMapperInstructions);
	}
	/**
	 * 
	 * Filters the hsearch index blob based on filter query given.
	 * @param inputBytes
	 * @param matchingIds
	 * @param filterQuery
	 * @param inputMapperInstructions
	 * @return returns a Map of key and value
	 * @throws IOException
	 */
	public static final Map<Integer, Object> process(final byte[] inputBytes, final BitSetWrapper matchingIds,
			final String  filterQuery,final HSearchProcessingInstruction inputMapperInstructions) throws IOException{
		
		try {
			IHSearchTable table = ScalarFilter.createTable(inputMapperInstructions);
			final Map<Integer, Object> rowContainer = new HashMap<Integer, Object>();
			
			table.get(inputBytes, new HSearchQuery(filterQuery), new MapperKVBaseEmptyImpl() {

				@Override
				public boolean onRowCols(BitSetWrapper k, Object v) {
					if ( null != matchingIds) k.and(matchingIds);
					for (int i = k.nextSetBit(0); i >= 0; i = k.nextSetBit(i+1)) {
						rowContainer.put(i, v);
					}
					return true;
				}

				@Override
				public boolean onRowCols(int k, Object value) {
					return true;
				}
			});
			
			return rowContainer;
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
}
