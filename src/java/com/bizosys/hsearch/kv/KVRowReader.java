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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.kv.impl.ScalarFilter;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class KVRowReader {

	
	public static final String COL_FAM = "1";
	public static final String INSTRUCTION = "kv";
	
	public static final byte[] getAllValues(final String tableName, final byte[] row, final String query, final int callBackType,  final int outputType) throws IOException {
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		byte[] storedBytes = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			
			scan.setStartRow(row);
			scan.setStopRow(row);

			HSearchProcessingInstruction outputTypeCode = new HSearchProcessingInstruction(callBackType, outputType, INSTRUCTION);
			ScalarFilter sf = new ScalarFilter(outputTypeCode, query);
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
