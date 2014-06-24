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
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Reads a given indexed map file and prints key and value size.
 * @author shubhendu
 *
 */
public class MapFileSizeReader {

	public static void main(String[] args) 
	{
		if(args.length < 1)
		{
			System.out.println("Usage: " + MapFileSizeReader.class + " <<hdfs-filepath>> <<key>>");
			System.exit(1);
		}
		
		String hdfsFilePath = args[0].trim();
		String askedKey = null;
		if(args.length == 2) askedKey = (args[1].trim());
		
		MapFile.Reader reader = null;
		try 
		{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(hdfsFilePath), conf);
			reader = new MapFile.Reader(fs, hdfsFilePath, conf);
			
			if ( null == askedKey) {
				Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),	conf);
				BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				
				while (reader.next(key, value)) 
				{
					if ( null == value) System.out.println(key.toString() + "\t0");
					System.out.println(key.toString() + "\t" + value.getLength());
				}
			} else {
				Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(),	conf);
				key.set(askedKey.getBytes());
				BytesWritable value = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
				
				reader.get(key, value); 
				System.out.println(key.toString() + "\t" + value.getLength());
			}
		} 
		catch (Exception e) 
		{
			System.err.println("Error in reading from HDFSFilepath:" + hdfsFilePath);
			e.printStackTrace(System.out);
		} 
		finally {
			IOUtils.closeStream(reader);
		}
		
	}

}
