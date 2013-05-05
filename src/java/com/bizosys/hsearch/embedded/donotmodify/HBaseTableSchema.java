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
package com.bizosys.hsearch.embedded.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.client.partition.PartitionByFirstLetter;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaCreator;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;

public class HBaseTableSchema {

	private static HBaseTableSchema singleton = null; 

	public static HBaseTableSchema getInstance() throws IOException {
		if ( null == singleton ) singleton = new HBaseTableSchema();
		return singleton;
	}
	
	@SuppressWarnings("rawtypes")
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
