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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.KVMapperLocal;
import com.bizosys.hsearch.kv.impl.KVMapperLocal.LocalMapContext;
import com.bizosys.hsearch.kv.impl.KVReducerLocal;
import com.bizosys.hsearch.kv.impl.KVReducerLocal.LocalReduceContext;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVIndexerLocal {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
	
    public void createTable(final String tableName, final String family){
		try {
			List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
			HColumnDescriptor cols = new HColumnDescriptor(family.getBytes());
			colFamilies.add(cols);
			HDML.create(tableName, colFamilies);
		} catch (HBaseException e) {
			e.printStackTrace();
		}
    }
    	
    public void index(String data, String schemaPath, boolean skipHeader) throws IOException, InterruptedException, ParseException {
    	index(data, schemaPath, skipHeader, null);
    }
	
    public void index(String data, String schemaPath, boolean skipHeader, String pluginClass) throws IOException, InterruptedException, ParseException {

    	FieldMapping fm = FieldMapping.getInstance(schemaPath);

    	//create table in hbase
    	System.out.println("Accessing HBase");
		HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
    	System.out.println(" Creating Table " + fm.tableName);
    	if ( !admin.tableExists(fm.tableName))
    		createTable(fm.tableName, fm.familyName);
    	System.out.println(fm.tableName + " Table is created");
    	
		
		KVMapperLocal mapper = new KVMapperLocal();
		LocalMapContext ctxM = mapper.getContext();
		ctxM.getConfiguration().set(KVIndexer.XML_FILE_PATH, schemaPath);
		if ( null != pluginClass)
			ctxM.getConfiguration().set(KVIndexer.PLUGIN_CLASS_NAME, pluginClass);
		
		mapper.setupRouter(ctxM);

		List<String> records = new ArrayList<String>();
		LineReaderUtil.fastSplit(records, data, '\n');
		
		long i = 1;
		for (String reacord : records) {
			if ( skipHeader ) {
				skipHeader = false;
				continue;
			}
			mapper.mapRouter(new LongWritable(i++), new Text(reacord), ctxM);
		}
		
		KVReducerLocal reducer = new KVReducerLocal();
		LocalReduceContext ctxR = reducer.getContext();
		ctxR.getConfiguration().set(KVIndexer.XML_FILE_PATH, schemaPath);
		if ( null != pluginClass)
			ctxR.getConfiguration().set(KVIndexer.PLUGIN_CLASS_NAME, pluginClass);
		reducer.setupRouter(ctxR);
		
		for (String key : ctxM.values.keySet()) {
			reducer.reduceRouter(new Text(key), ctxM.values.get(key), ctxR);
		}
		
		System.out.println("Data is uploaded sucessfully");
    }	

}
