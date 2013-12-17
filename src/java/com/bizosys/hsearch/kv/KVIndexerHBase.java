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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.KVMapperHBase;
import com.bizosys.hsearch.kv.impl.KVReducer;

public class KVIndexerHBase {

	public static Map<String, Character> dataTypesPrimitives = new HashMap<String, Character>();
	static {
		dataTypesPrimitives.put("string", 't');
		dataTypesPrimitives.put("text", 'e');
		dataTypesPrimitives.put("int", 'i');
		dataTypesPrimitives.put("float", 'f');
		dataTypesPrimitives.put("double", 'd');
		dataTypesPrimitives.put("long", 'l');
		dataTypesPrimitives.put("short", 's');
		dataTypesPrimitives.put("boolean", 'b');
		dataTypesPrimitives.put("byte", 'c');
	}

    public void execute( String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
    	execute(new KVMapperHBase(), new KVReducer(), args);
    }
	
	public void execute(KVMapperHBase map, KVReducer reduce, String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
 
    	if(args.length < 2){
            System.out.println("Please enter valid number of arguments.");
            System.out.println("Usage : KVIndexer <<Input Table>> <<XML File Configuration>>");
            System.exit(1);
        }
    	
    	String inputTable = args[0];
    	String schemaPath = args[1];

    	if (null == inputTable || inputTable.trim().isEmpty()) {
            System.out.println("Please enter proper table");
            System.exit(1);
        }
 
		Configuration conf = HBaseConfiguration.create();

		StringBuilder sb = new StringBuilder(8192);
		Path hadoopPath = new Path(schemaPath);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
		String line = null;
		while((line = br.readLine())!=null) {
			sb.append(line);
		}

		br.close();
		FieldMapping fm = new FieldMapping();
		fm.parseXMLString(sb.toString());
		
		if ( fm.tableName.equals(inputTable)) {
			throw new IOException("Input table and index table can not be same");
		}

    	//create table in hbase
		HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
		String outputTableName = fm.tableName;
    	if ( !admin.tableExists(outputTableName))
    		createTable(outputTableName, fm.familyName);

		conf.set(KVIndexer.XML_FILE_PATH, schemaPath);
		
		Job job = new Job(conf,"KVIndexerHBase");
		job.setJarByClass(KVIndexerHBase.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan = scan.addFamily(fm.familyName.getBytes());
		
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
			inputTable,        // input table
			scan,               // Scan instance to control CF and attribute selection
			map.getClass(),     // mapper class
			Text.class,         // mapper output key
			Text.class,  // mapper output value
			job);
		
		TableMapReduceUtil.initTableReducerJob(
			outputTableName,        // output table
			reduce.getClass(),    // reducer class
			job);
		job.setNumReduceTasks(1);   // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
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
    	    
    
    public static void main(String[] args) throws Exception {
		new KVIndexerHBase().execute(new KVMapperHBase(), new KVReducer(), args);
	}

}
