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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.dao.MapperKV;
import com.bizosys.hsearch.kv.dao.RowReader;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.kv.impl.BitSetRow;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.kv.impl.Datatype;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

/**
 * A database reader class that reads the hsearch index.  
 * @author shubhendu
 *
 */
public abstract class DatabaseReader implements RowReader{
	
	
	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	@Override
	public byte[] readStoredProcedureBlob(final String tableName, 
			final byte[] row, final byte[] matchingIdsB, 
			final BitSetWrapper matchingIds, final String filterQuery, 
			final HSearchProcessingInstruction instruction) throws IOException {
		
		ComputeKV compute = new ComputeKV();
		compute.kvType = (instruction.getOutputType() == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : instruction.getOutputType();
		compute.kvRepeatation = instruction.getProcessingHint().startsWith("true");
		compute.isCompressed = instruction.getProcessingHint().endsWith("true");
		
		return readStoredProcedureBlob(tableName, row, matchingIdsB, matchingIds, filterQuery, instruction);
	}
	
	@Override
	public byte[] readStoredProcedureBlob(final String tableName, 
		final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
		final BitSetWrapper matchingIds, final String filterQuery, 
		final HSearchProcessingInstruction instruction) throws IOException {

		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DatabaseReader:readStoredProcedureBlob\t" + tableName + " , " + new String(row));
		}
		
		byte[] storedBytes = readRowBlob(tableName, row);
		if ( null == storedBytes) return null;
		return filterBlob(matchingIdsB, filterQuery, instruction, storedBytes);
	}

	@Override
	public Map<Integer, Object> readStoredProcedure(final String tableName, 
		final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
		final BitSetWrapper matchingIds, final String filterQuery, 
		final HSearchProcessingInstruction instruction) throws IOException {

		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DatabaseReader:readStoredProcedure\t" + tableName + " , " + new String(row));
		}
		
		byte[] storedBytes = readRowBlob(tableName, row);
		int storedBytesLen = ( null == storedBytes) ? 0 : storedBytes.length;
		if ( storedBytesLen == 0 ) return new HashMap<Integer, Object>(0);
		
		if(compute.kvRepeatation) {
			return BitSetRow.process(storedBytes,matchingIds,filterQuery,instruction);
		} else{
			compute.rowContainer = new HashMap<Integer, Object>();
			compute.parse(storedBytes, matchingIds);
			return compute.rowContainer;				
		}
	}	
	

	@Override
	public Map<Integer, Object> readRow(
			final String tableName, final byte[] row, final ComputeKV compute,
			final String filterQuery,
			final HSearchProcessingInstruction inputMapperInstructions) throws IOException {
		
		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DatabaseReader:readRow\t" + tableName + " , " + new String(row));
		}
		
		byte[] storedBytes = readRowBlob(tableName, row);
		return parseBlob(storedBytes,filterQuery, compute, null, inputMapperInstructions ); 
	}
		
	public static Map<Integer, Object> parseBlob(final byte[] storedBytes,final String filterQuery,
			final ComputeKV compute, final byte[] matchingIdsB,
			final HSearchProcessingInstruction instruction ) throws IOException {
		
		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DatabaseReader:parseBlob\t" + filterQuery);
		}
		
		int storedBytesLen = ( null == storedBytes) ? 0 : storedBytes.length;
		if ( storedBytesLen == 0 ) return new HashMap<Integer, Object>(0);
		if(compute.kvRepeatation) {
			return BitSetRow.process(storedBytes,filterQuery,instruction);
		} else{
			if ( null == compute.rowContainer ) 
				compute.rowContainer = new HashMap<Integer, Object>(0);
			compute.parse(storedBytes, BitSetWrapper.valueOf(storedBytes));
			return compute.rowContainer;
		}
	}
	
	public static byte[] filterBlob(final byte[] matchingIdsB, final String filterQuery, 
			final HSearchProcessingInstruction instruction, byte[] storedBytes)
			throws IOException {
		
		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DatabaseReader:filterBlob\t" + filterQuery);
		}
		
		IHSearchTable table = ScalarFilter.createTable(instruction);	
		IHSearchPlugin plugin = new MapperKV();
		plugin.setOutputType(instruction);
		if(0 != matchingIdsB.length) plugin.setMergeId(matchingIdsB);
		HSearchQuery hQuery = null;
		try {
			hQuery = new HSearchQuery(filterQuery);
		} catch (java.text.ParseException e) {
			throw new IOException("Parse Exception , " + filterQuery , e);
		}
		
		switch ( instruction.getCallbackType()) {
			case HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS:
				table.get(storedBytes, hQuery, plugin);
				break;
			case HSearchProcessingInstruction.PLUGIN_CALLBACK_ID:
				table.keySet(storedBytes, hQuery, plugin);
				break;
			case HSearchProcessingInstruction.PLUGIN_CALLBACK_VAL:
				table.values(storedBytes, hQuery, plugin);
				break;
			case HSearchProcessingInstruction.PLUGIN_CALLBACK_IDVAL:
				table.keyValues(storedBytes, hQuery, plugin);
				break;
			default:
				throw new IOException("Unknown output type:" + instruction.getCallbackType());
		}	
		
		Collection<byte[]> dataCarrier = new ArrayList<byte[]>();
		plugin.getResultSingleQuery(dataCarrier);
		return SortedBytesArray.getInstanceArr().toBytes(dataCarrier);
	}
	
	public 	abstract DatabaseReader get();
	
}
