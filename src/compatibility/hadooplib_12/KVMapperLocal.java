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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.indexing.KVIndexer;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVMapperLocal extends Mapper<LongWritable, Text, TextPair, Text> {
    	
	public class LocalMapContext extends Context {

		Configuration conf = null;
		public Map<TextPair, List<Text>> values = new HashMap<TextPair, List<Text>>();

		public LocalMapContext(Configuration conf, TaskAttemptID taskid,
				RecordReader<LongWritable, Text> reader,
				RecordWriter<TextPair, Text> writer, OutputCommitter committer,
				StatusReporter reporter, InputSplit split) throws IOException,
				InterruptedException {
			super(new Configuration(), new TaskAttemptID(), reader, writer, committer, reporter, split);
			this.conf = super.getConfiguration();
		}

		
		
		@Override
		public InputSplit getInputSplit() {
			return null;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public OutputCommitter getOutputCommitter() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public void write(TextPair key, Text val) throws IOException,InterruptedException {
			if ( values.containsKey(key)) {
				values.get(key).add(val);
			} else {
				List<Text> keyvalList = new ArrayList<Text>();
				keyvalList.add(val);
				values.put(key, keyvalList);
			}
		}

		@Override
		public Counter getCounter(Enum<?> arg0) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Counter getCounter(String arg0, String arg1) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public float getProgress() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public String getStatus() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void setStatus(String arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Configuration getConfiguration() {
			return conf;
		}

		@Override
		public Credentials getCredentials() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RawComparator<?> getGroupingComparator() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getJar() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public JobID getJobID() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String getJobName() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<?> getMapOutputKeyClass() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public int getNumReduceTasks() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<?> getOutputValueClass() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public RawComparator<?> getSortComparator() {
			// TODO Auto-generated method stub
			return null;
		}


		@Override
		public Path getWorkingDirectory() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public void progress() {
			// TODO Auto-generated method stub
			
		}

	}
	
	public LocalMapContext getContext() throws IOException, InterruptedException {
		return new LocalMapContext(null,null,null,null,null,null,null);
	}

	
	private KVMapperBase kBase = new KVMapperBase();
	String[] result = null; 

	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		kBase.setup(context);
	}
	
    public void setupRouter(Context context) throws IOException, InterruptedException {
    	this.setup(context);
    }
	
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	if ( null == result) {
    		ArrayList<String> resultL = new ArrayList<String>();
    		LineReaderUtil.fastSplit(resultL, value.toString(), KVIndexer.FIELD_SEPARATOR);
    		result = new String[resultL.size()];
    	}
    	Arrays.fill(result, null);

    	LineReaderUtil.fastSplit(result, value.toString(), KVIndexer.FIELD_SEPARATOR);
    	
    	kBase.map(result, context);
    }
    
    public void mapRouter(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	this.map(key, value, context);
    }
}
