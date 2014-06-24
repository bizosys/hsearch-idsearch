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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class DataBlockHBase extends DatabaseReader{

	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	public static final String COL_FAM = "1";
	
	@Override
	public final  byte[] readRowBlob(final String tableName, final byte[] row) throws IOException {
		
		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DataBlockHBase:readRowBlob\t" + tableName + " , " + new String(row));
		}
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		byte[] storedBytes = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(false);
			scan.setMaxVersions(1);
			
			scan.setStartRow(row);
			scan.setStopRow(row);
	    	
			scanner = table.getScanner(scan);
			Result r = scanner.iterator().next();
			
			if(null == r) return null;
			
			storedBytes = r.getValue(COL_FAM.getBytes(), new byte[]{0});
			return storedBytes;
			
		} catch ( Exception ex) {
			throw new IOException("Error Accessing " + tableName + ", Row " + new String(row), ex );
		} finally {
			if ( null != scanner) try { scanner.close(); } catch (Exception ex) {};
			if ( null != table ) try { facade.putTable(table); } catch (Exception ex) {};
		}		
	}

	@Override
	public byte[] readStoredProcedureBlob(final String tableName, 
		final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
		final BitSetWrapper matchingIds, final String filterQuery, 
		final HSearchProcessingInstruction instruction) throws IOException {

		if (DEBUG_ENABLED) {
			IdSearchLog.l.debug("DataBlockHBase:readStoredProcedureBlob\t" + tableName + " , " + new String(row));
		}
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		byte[] storedBytes = null;
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(1);
			scan.setMaxVersions(1);
			
			scan.setStartRow(row);
			scan.setStopRow(row);

			ScalarFilter sf = new ScalarFilter(instruction, filterQuery);
			sf.setMatchingIds(matchingIdsB);
	    	scan.setFilter(sf);
	    	
			scanner = table.getScanner(scan);
			Result r = scanner.iterator().next();
			storedBytes = (null == r) ? null : r.getValue(COL_FAM.getBytes(), new byte[]{0});

			if (DEBUG_ENABLED) {
				int storedBytesT = ( null == storedBytes) ? 0 : storedBytes.length;
				IdSearchLog.l.debug("DataBlockHBase:readStoredProcedureBlob\t" + tableName + " , " + new String(row) + " , " + storedBytesT);
			}
			
			return storedBytes;
			
		} catch ( IOException ex) {
			throw ex;
		} finally {
			if ( null != scanner) try { scanner.close(); } catch (Exception ex) {};
			if ( null != table ) try { facade.putTable(table); } catch (Exception ex) {};
		}
		
	}

	@Override
	public Map<Integer, Object> readStoredProcedure(final String tableName, 
		final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
		final BitSetWrapper matchingIds, final String filterQuery, 
		final HSearchProcessingInstruction instruction) throws IOException {

	
		byte[] storedBytes = readStoredProcedureBlob(tableName, 
				row, compute, matchingIdsB, matchingIds, filterQuery,instruction);

		Map<Integer, Object> finalResult = new HashMap<Integer, Object>(); 
		compute.rowContainer = finalResult;
		compute.put(storedBytes);
		return finalResult;
		
	}

	@Override
	public DatabaseReader get() {
		return this;
	}	
	

}
