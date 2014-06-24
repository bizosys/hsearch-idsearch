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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileUtil {
	
	public static final class Writer {
		
		MapFile.Writer writer = null;
		Configuration conf = null;
		
		public void setConfiguration(Configuration conf) {
			this.conf = conf;
		}
		
		public void open(Class<? extends WritableComparable<?>> keyClass, Class<?> valueClass,String outputDir, CompressionType compressionType) throws IOException{
			
			if(null == conf)
				conf = new Configuration();
			
			FileSystem fs = FileSystem.get(conf);
			writer = new MapFile.Writer(conf, fs, outputDir, keyClass, valueClass, compressionType);

		}
		
		public void append(WritableComparable<?> key, Writable val) throws IOException{
			writer.append(key, val);
		}
		
		public void close() throws IOException{
			
			if(null != writer)
				writer.close();
		}
		
	}

	public static final class Reader {
		
		MapFile.Reader reader = null;
		Configuration conf = null;
		Writable val = null;
		WritableComparable<?> key = null;
		
		public void setConfiguration(Configuration conf) {
			this.conf = conf;
		}
		
		public void open(String outputDir) throws IOException{

			if(null == conf)
				conf = new Configuration();

			FileSystem fs = FileSystem.get(conf);
			this.reader = new MapFile.Reader(fs, outputDir, conf);
			this.key = (WritableComparable<?>) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			this.val = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		}
		
		public void get(WritableComparable<?> key, Writable val) throws IOException{
			reader.get(key, val);
		}
		
		public void getAll(Map<WritableComparable<?>, Writable> container) throws IOException{
			while (reader.next(key, val)) {
				container.put(key, val);
			}
		}
		
		public boolean next(WritableComparable<?> key, Writable val) throws IOException{
			return reader.next(key, val);
		}
		
		public WritableComparable<?> getKeyClass(){
			return this.key;
		}
		
		public Writable getValueClass(){
			return this.val;
		}

		public void close() throws IOException{
			
			if(null != reader)
				reader.close();
		}
	}
	
	public static void main(String[] args) {
	}

}
