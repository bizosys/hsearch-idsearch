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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RawKeyValueIterator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.Progress;

import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HTableWrapper;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.indexing.KVIndexer;

public class KVReducerLocal extends TableReducer<TextPair, Text, ImmutableBytesWritable> {
    
	public class RawKeyValueIteratorImpl implements RawKeyValueIterator {
		@Override
		public void close() throws IOException {}
		@Override
		public DataInputBuffer getKey() throws IOException {return null;}
		@Override
		public Progress getProgress() {return null;}
		@Override
		public DataInputBuffer getValue() throws IOException {return null;}
		@Override
		public boolean next() throws IOException {return false;}
	}
	
	public class LocalReduceContext extends Context {

		public LocalReduceContext(
				Configuration conf,
				TaskAttemptID taskid,
				RawKeyValueIterator input,
				Counter inputKeyCounter,
				Counter inputValueCounter,
				org.apache.hadoop.mapreduce.RecordWriter<ImmutableBytesWritable, Writable> output,
				OutputCommitter committer, StatusReporter reporter,
				RawComparator<TextPair> comparator, Class<TextPair> keyClass,
				Class<Text> valueClass) throws IOException,
				InterruptedException {
			
			super(new Configuration(), new TaskAttemptID(), 
					new RawKeyValueIteratorImpl(), inputKeyCounter, inputValueCounter, output,
					committer, reporter, comparator, keyClass, valueClass);
			this.conf = super.getConfiguration();
		}

		Configuration conf = null;
		String tableName = null;


		
		@Override
		public Iterable<Text> getValues() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean nextKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			return false;
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
		public synchronized void write(ImmutableBytesWritable arg0, Writable arg1) throws IOException, InterruptedException {

			if ( null == tableName) {
				String path = conf.get(KVIndexer.XML_FILE_PATH);
				tableName = KVIndexer.createFieldMapping(conf, path, new StringBuilder()).tableName;
			}
			
			Put updates = (Put) arg1;
			
			HTableWrapper table = null;
			HBaseFacade facade = null;
			try {
				facade = HBaseFacade.getInstance();
				table = facade.getTable(tableName);
				table.put(updates);
				table.flushCommits();
			} finally {
				if ( null != facade && null != table) {
					facade.putTable(table);
				}
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
			return this.conf;
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
	
	public LocalReduceContext getContext() throws IOException, InterruptedException {
		return new LocalReduceContext(null,null,null,null,null,null,null,null,null,TextPair.class,Text.class);
	}
	
	KVReducerHBase reducer = new KVReducerHBase();

	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		reducer.setup(context);
	}
	
    public void setupRouter(Context context) throws IOException, InterruptedException {
    	this.setup(context);
    }	
	
	@Override
    protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		reducer.reduce(key, values, context);
    }

    public void reduceRouter(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	this.reduce(key, values, context);
    }	

}