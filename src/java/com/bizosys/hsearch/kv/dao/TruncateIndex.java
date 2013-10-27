package com.bizosys.hsearch.kv.dao;

import java.util.List;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.kv.impl.FieldMapping;

public class TruncateIndex {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		FieldMapping fm = FieldMapping.getInstance(args[0]);
		String tableName = fm.tableName;
		String family = fm.familyName;
		
		HBaseFacade facade = null;
		ResultScanner scanner = null;
		HTableWrapper table = null;
		List<byte[]> matched = null;
		try {
			facade = HBaseFacade.getInstance();
			table = facade.getTable(tableName);
			
			Scan scan = new Scan();
			scan.setCacheBlocks(true);
			scan.setCaching(500);
			scan.setMaxVersions(1);
			scan = scan.addFamily(family.getBytes());
			scanner = table.getScanner(scan);
			
			for (Result r: scanner) {
				if ( null == r) continue;
				if ( r.isEmpty()) continue;
				Delete delete = new Delete(r.getRow());
				table.delete(delete);
			}
		} finally {
			if ( null != scanner) scanner.close();
			if ( null != table ) {
				table.flushCommits();
				facade.putTable(table);
			}
			if ( null != matched) matched.clear();
		}

	}

}
