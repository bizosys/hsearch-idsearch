package com.bizosys.hsearch.storage.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.client.partition.PartitionNumeric;
import com.bizosys.hsearch.treetable.client.partition.PartitionByFirstLetter;

import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaCreator;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;

public class HBaseTableSchema {

	private static HBaseTableSchema singleton = null; 

	public static HBaseTableSchema getInstance() throws IOException {
		if ( null == singleton ) singleton = new HBaseTableSchema();
		return singleton;
	}
	
	private HBaseTableSchema() throws IOException {
		
		HBaseTableSchemaDefn.getInstance().tableName = "hsearch-index";
		Map<String, IPartition> columns = HBaseTableSchemaDefn.getInstance().columnPartions;
		columns.put("Documents",new PartitionByFirstLetter());
		columns.get("Documents").setPartitionsAndRange(
			"Documents",
			"0,1,2,3,4,5,6,7,8,9",
			"[*:1],[1:2],[2:3],[3:4],[4:5],[5:6],[6:7],[7:8],[8:9],[9:*]",
			2);

	}

	public HBaseTableSchemaDefn getSchema() {
		return HBaseTableSchemaDefn.getInstance();
	}
	
	public void createSchema() {
		new HBaseTableSchemaCreator().init();
	}
	
	public static void main(String[] args) throws Exception {
		HBaseTableSchema.getInstance().createSchema();
	}
}