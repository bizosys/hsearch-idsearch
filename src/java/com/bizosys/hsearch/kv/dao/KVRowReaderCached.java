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

package com.bizosys.hsearch.kv.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.impl.BitSetRow;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class KVRowReaderCached implements KvRowReaderFactory.RowReader{

	protected KVRowReaderCached() {
	}

	@Override
	public final Map<Integer, Object> getAllValues(
		final String tableName, final byte[] row, 
		final ComputeKV compute, final String filterQuery,
		final HSearchProcessingInstruction inputMapperInstructions) throws IOException {
			
		byte[] storedBytes = DataBlock.getBlock(tableName, new String(row), true);
		return KVRowReaderIpc.extractAllValues(compute, filterQuery, inputMapperInstructions,storedBytes);
	}
	
	
	@Override
	public final Map<Integer, Object> getFilteredValues(final String tableName, 
			final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
			final BitSetWrapper matchingIds,final String filterQuery, 
			final HSearchProcessingInstruction instruction) throws IOException {
		
		byte[] storedBytes = DataBlock.getBlock(tableName, new String(row), true);
		if ( null == storedBytes) return null;
		
		if(compute.kvRepeatation) {
			return BitSetRow.process(storedBytes,matchingIds,filterQuery,instruction);
		} else{
			compute.rowContainer = new HashMap<Integer, Object>();
			compute.parse(storedBytes, matchingIds);
			return compute.rowContainer;				
		}
	}
}
