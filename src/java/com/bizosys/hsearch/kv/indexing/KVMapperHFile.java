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
package com.bizosys.hsearch.kv.indexing;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bizosys.hsearch.kv.impl.FieldMapping;

public class KVMapperHFile extends Mapper<Text, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue>{

	
	FieldMapping fm = null;
	byte[] familyName = null;
	byte[] qualifier = new byte[]{0};

	ImmutableBytesWritable hKey = new ImmutableBytesWritable();

	@Override
	protected void setup(Context context)throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		fm = KVIndexer.createFieldMapping(conf, path, new StringBuilder());
		familyName = fm.familyName.getBytes();
	}


	@Override
	protected void map(Text key, ImmutableBytesWritable value,Context context){

		try{
			
			String rowKey = key.toString();
			byte[] data = value.copyBytes();
			hKey.set(rowKey.getBytes());
			KeyValue kv = new KeyValue(hKey.get(), familyName, qualifier, data);
			context.write(hKey, kv);
			
		} catch(Exception e){
			
			System.err.println("Error in processing for row key : " + key.toString() 
					+ "\t and value size " + value.getLength() 
					+ "\n Memory total:max:free(MB) " + 
					Runtime.getRuntime().totalMemory()/1024 * 1024 + " : " + 
					Runtime.getRuntime().maxMemory()/1024 * 1024 + " : " +
					Runtime.getRuntime().freeMemory()/1024 * 1024 + " : ");
		}
		
	}
}
