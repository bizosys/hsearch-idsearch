package com.bizosys.hsearch.kv.impl;

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
import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.unstructured.util.IdSearchLog;

public class KVReducerLocal extends TableReducer<Text, Text, ImmutableBytesWritable> {
    
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
				RawComparator<Text> comparator, Class<Text> keyClass,
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
				tableName = this.conf.get(KVIndexer.TABLE_NAME);
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
		return new LocalReduceContext(null,null,null,null,null,null,null,null,null,Text.class,Text.class);
	}
	
	KVReducer reducer = new KVReducer();

	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		reducer.setup(context);
	}
	
    public void setupRouter(Context context) throws IOException, InterruptedException {
    	this.setup(context);
    }	
	
	@Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		reducer.reduce(key, values, context);
    }

    public void reduceRouter(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	this.reduce(key, values, context);
    }	

}
