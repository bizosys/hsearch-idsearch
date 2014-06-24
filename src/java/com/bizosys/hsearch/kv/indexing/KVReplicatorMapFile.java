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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.idsearch.util.MapFileUtil;
import com.bizosys.hsearch.kv.impl.FieldMapping;

public class KVReplicatorMapFile  extends Configured implements Tool{

	public static String OUTPUT_FILE_PATH = "output-filepath";
	public static String OUTPUT_FILE_NAME = "output-filename";
	public static String REPLACE_FROM = "replace-from";
	public static String REPLACE_TO = "replace-to";
	public static String START_INDEX = "start-index";
	public static String END_INDEX = "end-index";

	public static class KVReplicatorMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{

		String replaceFrom =  "";
		String replaceTo = "";
		int startIndex = Integer.MIN_VALUE;
		int endIndex = Integer.MIN_VALUE;

		@Override
		protected void setup(Context context)throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();

			replaceFrom =  conf.get(REPLACE_FROM);
			replaceTo = conf.get(REPLACE_TO);
			startIndex = conf.getInt(START_INDEX, Integer.MIN_VALUE);
			endIndex = conf.getInt(END_INDEX, Integer.MIN_VALUE);
		}


		@Override
		protected void map(Text key, BytesWritable value,Context context) throws IOException, InterruptedException{

			String rowKey = key.toString();
			String keyStr = rowKey.replace(replaceFrom, replaceTo);
			int index = keyStr.indexOf("[n]");
			if ( index == -1 )return;

			String firstPart = keyStr.substring(0, index);
			String lastPart = keyStr.substring(index + 3);
			String newKey = null;
			String siteName = null;
			
			for ( int i= startIndex ; i<endIndex ; i++) {
				siteName = firstPart + Integer.toString(i);
				newKey = siteName + lastPart;
				context.write(new Text(newKey), value);
			}
		}
	}

	public static class KVReplicatorReducer extends Reducer<Text, BytesWritable, Text, BytesWritable>{

		String outputFileName = null;

		MapFileUtil.Writer writer = null;
		MapFileUtil.Writer metaWriter = null;

		private static final String MAPPING_FILE = "mapping";
		
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			outputFileName = conf.get(KVReplicatorMapFile.OUTPUT_FILE_NAME);

			String parentOutputFile = conf.get(KVReplicatorMapFile.OUTPUT_FILE_PATH);
			parentOutputFile = parentOutputFile.charAt(parentOutputFile.length() - 1) == '/' ? 
					parentOutputFile : parentOutputFile + "/";

			String dataFilePath = parentOutputFile + outputFileName;
			String mappingFilePath = parentOutputFile + MAPPING_FILE;
			
			writer = new MapFileUtil.Writer();
			writer.setConfiguration(conf);
			writer.open(Text.class, BytesWritable.class, dataFilePath, CompressionType.NONE);

			metaWriter = new MapFileUtil.Writer();
			metaWriter.setConfiguration(conf);
			metaWriter.open(Text.class, Text.class, mappingFilePath, CompressionType.NONE);


		}
		
		@Override
		protected void reduce(Text key, Iterable<BytesWritable> values, Context arg2)
				throws IOException, InterruptedException {

			for (BytesWritable byteChunk : values) {
	    		writer.append(key, byteChunk);
	    		metaWriter.append(key, new Text(outputFileName));
			}
			
		}
		
		@Override
		protected void cleanup(Context context)throws IOException, InterruptedException {
			if(null != writer)
				writer.close();
			if(null != metaWriter)
				metaWriter.close();
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {

		int seq = 0;
		String inputFile = ( args.length > seq ) ? args[seq] : "";
		seq++;

    	String outputFile = ( args.length > seq ) ? args[seq++] : "/tmp/hsearch-index";

    	String outputFileName = ( args.length > seq ) ? args[seq++] : "file1";
    	
    	String xmlFilePath = ( args.length > seq ) ? args[seq++] : "";

		String replaceFrom = ( args.length > seq ) ? args[seq++] : "";

		String replaceTo = ( args.length > seq ) ? args[seq++] : "";

		String startIndex = ( args.length > seq ) ? args[seq++] : "";

		String endIndex = ( args.length > seq ) ? args[seq++] : "";

		String numberOfReducerStr = ( args.length > seq ) ? args[seq] : "1";
		int numberOfReducer = Integer.parseInt(numberOfReducerStr);
		
		if (null == inputFile || inputFile.trim().isEmpty()) {
			String err = KVReplicatorHFile.class + " > Please enter input file path.";
			System.err.println(err);
			throw new IOException(err);
		}


		Configuration conf = HBaseConfiguration.create();

		FieldMapping fm = KVIndexer.createFieldMapping(conf, xmlFilePath, new StringBuilder());
       	outputFile = outputFile.charAt(outputFile.length() - 1) == '/' ? outputFile : outputFile + "/";
		outputFile = outputFile + fm.tableName;

		conf.set(OUTPUT_FILE_PATH, outputFile);
		conf.set(OUTPUT_FILE_NAME, outputFileName);

		conf.set(REPLACE_FROM, replaceFrom);
		conf.set(REPLACE_TO, replaceTo);
		conf.set(START_INDEX, startIndex);
		conf.set(END_INDEX, endIndex);

		Job job = Job.getInstance(conf, "KVReplicatorMapFile - Replicating Map File");

		job.setJarByClass(KVReplicatorMapFile.class);
		job.setMapperClass(KVReplicatorMapper.class);
		job.setReducerClass(KVReplicatorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setNumReduceTasks(numberOfReducer);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		SequenceFileInputFormat.addInputPath(job, new Path(inputFile.trim()));

		FileSystem fs = FileSystem.get(conf);
        Path dummyPath = new Path("/tmp", "dummy");
        if(fs.exists(dummyPath)){
        	fs.delete(dummyPath, true);
        }
        
        FileOutputFormat.setOutputPath(job, dummyPath);
		
		boolean result = job.waitForCompletion(true);
		return (result ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {

		if(args.length < 8){
			String err = "\nUsage : "+ KVReplicatorMapFile.class +" <<Input File Path>> <<Output File Path>> <<Output File name>> <<XML File Configuration>> <<replace from>> <<replace to[n]>> <<start n>> <<end n>> <<number of reducer>>";
			IdSearchLog.l.fatal(err);
			System.exit(1);
		}

		int exitCode = ToolRunner.run(new Configuration(),new KVReplicatorMapFile(), args);
		System.exit(exitCode);
	}

}
