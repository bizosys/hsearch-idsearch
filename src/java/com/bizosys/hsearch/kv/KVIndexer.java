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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import com.bizosys.hsearch.kv.impl.KVMapper;
import com.bizosys.hsearch.kv.impl.KVPlugin;
import com.bizosys.hsearch.kv.impl.KVReducer;
import com.bizosys.unstructured.util.IdSearchLog;

public class KVIndexer {

	public static String XML_FILE_PATH = "CONFIG_XMLFILE_LOCATION";
	public static String PLUGIN_CLASS_NAME = "PLUGIN_CLASS_NAME";
	public static final String MERGEKEY_ROW = "--HSEARCH_PARTITION_KEYS--";	
	public static String SKIP_HEADER = "false";
	public static String TABLE_NAME = "Table";
	public static final String INCREMENTAL_ROW = "auto";
	public static char FIELD_SEPARATOR = '|';
	public static byte[] FAM_NAME = "1".getBytes();
	public static byte[] COL_NAME = new byte[]{0};

	public static class KV {

		public Object key;
		public Object value;

		public KV(Object key, Object value) {
			this.key = key;
			this.value = value;
		}
	}
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
	
	public static KVPlugin createPluginClass(Configuration conf) throws IOException{
		
		KVPlugin plugin = null;
		String pluginClassName = conf.get(KVIndexer.PLUGIN_CLASS_NAME);
		int isPlugin = (null == pluginClassName) ? 0 : pluginClassName.trim().length();
		if ( isPlugin == 0) return null;
		
		try {
			Object pluginObj = Class.forName(pluginClassName).newInstance();
			if ( pluginObj instanceof KVPlugin) {
				plugin = ( KVPlugin ) pluginObj;
			} else {
				throw new IOException("Unknown Plugin class - " + pluginClassName);
			}
		} catch (ClassCastException ex) {
			throw new IOException("Unknown Plugin class - " + pluginClassName, ex);
		} catch (InstantiationException e) {
			throw new IOException("Unknown Plugin class - " + pluginClassName, e);
		} catch (IllegalAccessException e) {
			throw new IOException("Unknown Plugin class - " + pluginClassName, e);
		} catch (ClassNotFoundException e) {
			throw new IOException("Unknown Plugin class - " + pluginClassName, e);
		}
		
		return plugin;
	}
	
    public static void main(String[] args) throws Exception{
    	new KVIndexer().execute(args);
    }
	

    public void execute( String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	execute(KVMapper.class, KVReducer.class, args);
    }
	
	public void execute( Class<? extends Mapper> map, Class<? extends TableReducer> reduce, String[] args) throws IOException, InterruptedException, ClassNotFoundException {
 
        String msg = this.getClass().getName() + " > Initializing indexer job.";
        System.out.println(msg);
        IdSearchLog.l.info(msg);
		
    	if(args.length < 3){
            String err = "Usage : KVIndexer <<Input File Path>> <<XML File Configuration>> <<Destination Table>>  <<Skip Header(true|false)>> <<Plugin VO Class Name>>";
            IdSearchLog.l.fatal(err);
            System.err.println(err);
            throw new IOException(err);
        }
    	
    	String inputFile = ( args.length > 0 ) ? args[0] : "";
    	String xmlFilePath = ( args.length > 1 ) ? args[1] : "";
    	String tableName = ( args.length > 2 ) ? args[2] : "";
    	String skipHeader = ( args.length > 3 ) ? args[3] : "false";
    	String pluginClassName = ( args.length > 4 ) ? args[4] : "";

    	if (null == inputFile || inputFile.trim().isEmpty()) {
            String err = this.getClass().getName() + " > Please enter input file path.";
            System.err.println(err);
            throw new IOException(err);
        }
 
		Configuration conf = HBaseConfiguration.create();
		conf.set(XML_FILE_PATH, xmlFilePath);
		conf.set(TABLE_NAME, tableName);
		conf.set(PLUGIN_CLASS_NAME, pluginClassName);
		conf.set(SKIP_HEADER, skipHeader);
		
        Job job = new Job(conf, "HSearch Key Value indexer");
        job.setJarByClass(this.getClass());
        job.setMapperClass(map);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(reduce);
 
        msg = this.getClass().getName() + " > Indexing from input file path " + inputFile;
        System.out.println(msg);
        IdSearchLog.l.info(msg);
        FileInputFormat.addInputPath(job, new Path(inputFile.trim()));
        
        TableMapReduceUtil.initTableReducerJob(args[2], reduce, job);
        msg = this.getClass().getName() + " > TableMapReduceUtil initialized " + inputFile;
        System.out.println(msg);
        IdSearchLog.l.info(msg);

        job.setNumReduceTasks(10);
        job.waitForCompletion(true);
    }
}	
