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
package com.bizosys.hsearch.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.idsearch.util.ICache;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.BitSetRow;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class DatabaseReaderCached extends DatabaseReader {

	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	DatabaseReader reader = null;
	boolean isCachable = true;
	
	public DatabaseReaderCached(DatabaseReader reader, boolean isCachable) {
		this.reader = reader;
		this.isCachable = isCachable;
	}
	
	@Override
	public final byte[] readRowBlob(final String tableName, final byte[] row) throws IOException {
		byte[] dataA = null;
		String rowId = new String(row);
		rowId = tableName + "\t" + rowId;
		
		if ( isCachable) {
		
			ICache cache = ICache.getInstance();
			if ( cache.containsKey(rowId)) {
				if (DEBUG_ENABLED) {
					IdSearchLog.l.debug("DatabaseReaderCache:readRowBlob\t" + tableName + " , " + new String(row));
				}				
				dataA = cache.get(rowId);
			} else {
				dataA = reader.readRowBlob(tableName, row);
				if (DEBUG_ENABLED) {
					int dataASize = ( null == dataA) ? 0 : dataA.length;
					IdSearchLog.l.debug("DatabaseReaderCache:put\t" + tableName + " , " + new String(row) + " , " + dataASize + " bytes");
				}				

				cache.put(rowId, dataA);
			}

		} else {
			dataA = reader.readRowBlob(tableName, row);
		}
		
		if ( null == dataA) return dataA;
		return dataA;

	}

	@Override
	public final Map<Integer, Object> readRow(final String tableName, final byte[] row,
			final ComputeKV compute, final String filterQuery,
			final HSearchProcessingInstruction inputMapperInstructions)
			throws IOException {
		
		byte[] storedBytes = readRowBlob(tableName, row);
		return parseBlob(storedBytes,filterQuery, compute, null, inputMapperInstructions ); 
	}

	@Override
	public final Map<Integer, Object> readStoredProcedure(final String tableName,
			final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
			final BitSetWrapper matchingIds, final String filterQuery,
			final HSearchProcessingInstruction instruction) throws IOException {
		
		if ( isCachable ) {
			if (DEBUG_ENABLED) {
				IdSearchLog.l.debug("DatabaseReaderCache:readStoredProcedure\t" + tableName + " , " + new String(row));
			}		
			byte[] storedBytes = readRowBlob(tableName, row);
			if ( null == storedBytes) return null;
			
			if(compute.kvRepeatation) {
				return BitSetRow.process(storedBytes,matchingIds,filterQuery,instruction);
			} else{
				compute.rowContainer = new HashMap<Integer, Object>();
				compute.parse(storedBytes, matchingIds);
				return compute.rowContainer;				
			}
			
		} else {
			return reader.readStoredProcedure(tableName, row, compute, 
				matchingIdsB, matchingIds, filterQuery, instruction);
		}

	}

	@Override
	public final byte[] readStoredProcedureBlob(final String tableName, final byte[] row,
			final ComputeKV compute, final byte[] matchingIdsB, final BitSetWrapper matchingIds,
			final String filterQuery, final HSearchProcessingInstruction instruction)
			throws IOException {

		if ( isCachable ) {
			if (DEBUG_ENABLED) {
				IdSearchLog.l.debug("DatabaseReaderCache:readStoredProcedureBlob\t" + tableName + " , " + new String(row));
			}				
			
			byte[] blob = readRowBlob(tableName, row);
			return DatabaseReader.filterBlob(matchingIdsB, filterQuery, instruction, blob);
		} else {
			return reader.readStoredProcedureBlob(tableName, row, compute, 
				matchingIdsB, matchingIds, filterQuery, instruction);
		}
	}

	@Override
	public final DatabaseReader get() {
		throw new RuntimeException("Not Applicable");
	}

}
