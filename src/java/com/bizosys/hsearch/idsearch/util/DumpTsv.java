/*
* Copyright 2010 Bizosys Technologies Limited
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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.util.LineReaderUtil;

public class DumpTsv {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if ( args.length < 5) {
			System.err.println("Usage " + DumpTsv.class.getName() + "  <<HBASE-TABLE>>  <<HBASE-FAMILY>>  <<IGNORE-COLUMNS-COMMASEPARATED>> <<COLUMENAME-SCAN-INTERVAL>> <<DUMP-FILE-NAME>> <<ROW-DUMP-LIMIT>> ");
			System.err.println("Use <<DUMP-FILE-NAME>> as none to list only the available columns.");
			return;
		}

		String tableName = args[0];
		byte[] familyB = args[1].getBytes();
		String ignoreCols = args[2];
		int jump = Integer.parseInt(args[3]);
		
		String fileName= args[4];
		int limit= Integer.parseInt(args[5]);

		Set<String> uniqueFielsNames = generateUniqueCols(tableName, familyB, jump);
		if ( null != ignoreCols) {
			List<String> ignoreColsL = new ArrayList<String>();
			LineReaderUtil.fastSplit(ignoreColsL, ignoreCols, ',');
			uniqueFielsNames.removeAll(ignoreColsL);
		}
		
		List<String> flds = new ArrayList<String>();
		flds.addAll(uniqueFielsNames);
		
		if ( "none".equals(fileName)) {
			for (String fld : flds) {
				System.out.println(fld);
			}
			return;
		}
		
		DataOutputStream out = null;
		try {
			out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
			StringBuilder sb = new StringBuilder();
			boolean isFirst = true;
			List<byte[]> fldsB = new ArrayList<byte[]>(flds.size());
			for (String fld : flds) {
				if ( isFirst ) isFirst = false;
				else sb.append("\t");
				sb.append(fld);
				fldsB.add(fld.getBytes());
			}
			byte[] header = sb.toString().getBytes();
			out.write(header, 0, header.length);
			
			download(tableName, familyB,  fldsB, limit, out);

		} catch(IOException ex) {
			ex.printStackTrace(System.err);
		} finally {
			out.flush();
			out.close();
		}
	}
	
	private static void download(String tableName, byte[] familyB,  Collection<byte[]> flds, int limit, DataOutputStream writer) throws IOException {

		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		System.out.println("Limit is set @ " + limit);
		System.out.println("Total Fields = " + flds.size());
		
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addFamily(familyB);

			scanner = table.getScanner(scan);
			int index = 0;
			StringBuilder sb = new StringBuilder();

			for (Result r: scanner) {

				if ( null == r) continue;
				if ( r.isEmpty()) continue;

				if ( index++ > limit) break;
				
				String value = null;
				boolean isFirst = true;
				for (byte[] fld : flds) {
					value = "";
					KeyValue columnLatest  = r.getColumnLatest(familyB, fld);
					if ( null != columnLatest) {
						byte[] data = columnLatest.getValue();
						if ( null != data) {
							value = new String(data);
							value = value.replace("\t", " ");
							value = value.replace("\n", " ");
							value = value.replace("\n\r", " ");
							value = value.replace("\r", " ");
						} 
					}
					if ( isFirst ) {
						sb.append('\n');
						isFirst = false;
					}
					else sb.append('\t');
					sb.append(value);
				}
				
				String data = sb.toString();
				if ( data.length() > 0 ) {
					byte[] dataB = data.getBytes();
					writer.write(dataB,0,dataB.length);
					sb.setLength(0);
				}
			}
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}
	

	private static Set<String> generateUniqueCols(String tableName, byte[] familyB,  int jump) throws IOException {

		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		Set<String> fields = new HashSet<String>(); 

		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addFamily(familyB);

			scanner = table.getScanner(scan);
			int jumpIndex = 0;
			for (Result r: scanner) {
				if ( jumpIndex++ < jump) continue;
				jumpIndex = 0;
				if ( null == r) continue;
				if ( r.isEmpty()) continue;

				for (KeyValue kv : r.list()) {
					fields.add(new String(new String(kv.getQualifier())));
				}
				
			}
			return fields;
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}
	}

}
