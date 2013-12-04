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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.idsearch.util.LruCache;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class DataBlock {
	
	public static final String COL_FAM = "1";

	public static String getDeleteId ( String mergeId) throws IOException {
		return mergeId + "[[[deletes]]]";
	}
	
	public static BitSetWrapper getPinnedBitSets (
		String tableName, String rowId) throws IOException {

		BitSetWrapper dataBits = null;
			
		LruCache cache = LruCache.getInstance();
		if ( cache.containsKey(rowId)) {
			dataBits = (BitSetWrapper) cache.getPinned(rowId);
		} else {
			
			byte[] dataA = getAllValuesIPC(tableName, rowId.getBytes());
			dataBits = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataA, 0, dataA.length);
			cache.putPinned(rowId, dataBits);
		}
		return dataBits;
	}
	
	public static byte[] getBlock (String tableName, String rowId, 
		boolean isCachable) throws IOException {

		byte[] dataA = null;
		
		if ( isCachable) {
		
			LruCache cache = LruCache.getInstance();
			if ( cache.containsKey(rowId)) {
				dataA = cache.get(rowId);
			} else {
				dataA = getAllValuesIPC(tableName, rowId.getBytes());
				cache.put(rowId, dataA);
			}

		} else {
			dataA = getAllValuesIPC(tableName, rowId.getBytes());
		}
		
		if ( null == dataA) return dataA;
		return dataA;
	}
	
	public static final byte[] getAllValuesIPC(
			final String tableName, final byte[] row) throws IOException {
			
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
	
	public static final byte[] getFilteredValuesIpc(final String tableName, 
			final byte[] row, final byte[] matchingIdsB, final String filterQuery, 
			final HSearchProcessingInstruction instruction) throws IOException {
			
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
				return storedBytes;
				
			} catch ( IOException ex) {
				throw ex;
			} finally {
				if ( null != scanner) try { scanner.close(); } catch (Exception ex) {};
				if ( null != table ) try { facade.putTable(table); } catch (Exception ex) {};
			}
		}
			
		
}
