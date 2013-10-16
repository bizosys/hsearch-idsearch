package com.bizosys.unstructured.util;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;

public class PrintRecordIds {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		if ( args.length < 2) {
			System.err.println("Usage " + PrintRecordIds.class.getName() + "  <<TABLE>>  <<FAMILY>>");
			return;
		}
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;

		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(args[0]);
		
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addFamily(args[1].getBytes());

			scanner = table.getScanner(scan);
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				
				for (KeyValue kv : r.list()) {
					System.out.println(new String(r.getRow()) + "\t" + new String(kv.getQualifier()) + "\t" + kv.getValueLength());
				}
			}
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) facade.putTable(table);
		}		

	}

}
