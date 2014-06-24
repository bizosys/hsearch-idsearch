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
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

/**
 * 
 * Thsis is used to read hsearch index row.
 *
 */
public interface RowReader {
	
	/**
	 * Given a index mergeid returns the hsearch index blob.  
	 * @param tableName
	 * @param row
	 * @return hsearch index blob
	 * @throws IOException
	 */
	byte[] readRowBlob(final String tableName, final byte[] row) throws IOException;
	
	/**
	 * Deserializes the hsearch index blob and returns the key value 
	 * for a given field based on the filterquery. 
	 * 
	 * @param tableName
	 * @param row
	 * @param compute
	 * @param filterQuery
	 * @param inputMapperInstructions
	 * @return Map of Key Value.
	 * @throws IOException
	 */
	Map<Integer, Object> readRow(
		final String tableName, final byte[] row, 
		final ComputeKV compute, final String filterQuery,
		final HSearchProcessingInstruction inputMapperInstructions) throws IOException;

	
	/**
	 * Deserializes the hsearch index blob and returns the key value 
	 * for a given field based on the filterquery and given matching ids. 
	 * 
	 * @param tableName
	 * @param row
	 * @param compute
	 * @param matchingIdsB
	 * @param matchingIds
	 * @param filterQuery
	 * @param instruction
	 * @return Map of Key Value
	 * @throws IOException
	 */
	Map<Integer, Object> readStoredProcedure(final String tableName, 
		final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
		final BitSetWrapper matchingIds, final String filterQuery, 
		final HSearchProcessingInstruction instruction) throws IOException;

	/**
	 * 
	 * @param tableName
	 * @param row
	 * @param compute
	 * @param matchingIdsB
	 * @param matchingIds
	 * @param filterQuery
	 * @param instruction
	 * @return
	 * @throws IOException
	 */
	byte[] readStoredProcedureBlob(final String tableName, 
			final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
			final BitSetWrapper matchingIds, final String filterQuery, 
			final HSearchProcessingInstruction instruction) throws IOException;
	
	/**
	 * 
	 * @param tableName
	 * @param row
	 * @param matchingIdsB
	 * @param matchingIds
	 * @param filterQuery
	 * @param instruction
	 * @return
	 * @throws IOException
	 */
	byte[] readStoredProcedureBlob(final String tableName, 
			final byte[] row, final byte[] matchingIdsB, 
			final BitSetWrapper matchingIds, final String filterQuery, 
			final HSearchProcessingInstruction instruction) throws IOException;	
	
}
