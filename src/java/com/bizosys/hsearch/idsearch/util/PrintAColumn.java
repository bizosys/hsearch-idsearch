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
package com.bizosys.hsearch.idsearch.util;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;

public class PrintAColumn {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if ( args.length < 2) {
			System.err.println("Usage " + PrintAColumn.class.getName() + "  <<TABLE>>  <<FAMILY>>  <<COLUMN-NAME>> <<PRINT-NULLS>>");
			return;
		}
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(args[0]);
			byte[] familyB = args[1].getBytes();
			byte[] colB = args[2].getBytes();
			boolean printNull = Boolean.parseBoolean(args[3]);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addFamily(familyB);

			scanner = table.getScanner(scan);
			String value = null;
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;

				value = "-";
				KeyValue columnLatest = r.getColumnLatest(familyB, colB);
				if ( null != columnLatest) {
					byte[] data = columnLatest.getValue();
					if ( null != data) {
						value = new String(data);
						int valueT = ( null == value) ? 0 : value.length();
						if ( 0 == valueT) if ( printNull ) continue;
					} else {
						if ( printNull ) continue;
					}
				} else {
					if ( printNull ) continue;
				}
				
				System.out.println(new String(r.getRow()) + "\t" + value);
			}
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}		

	}

}
