package com.bizosys.hsearch.kv.indexing;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVMapperLocal extends Mapper<LongWritable, Text, TextPair, Text> {
    	
	public class LocalMapContext extends Context {

		public Map<TextPair, List<Text>> values = new HashMap<TextPair, List<Text>>();
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
		public Path[] getArchiveClassPaths() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String[] getArchiveTimestamps() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public URI[] getCacheArchives() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public URI[] getCacheFiles() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {
			// TODO Auto-generated method stub
			return null;
		}

		Configuration conf = new Configuration();
		
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
		public Path[] getFileClassPaths() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String[] getFileTimestamps() {
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
		public boolean getJobSetupCleanupNeeded() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Path[] getLocalCacheArchives() throws IOException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Path[] getLocalCacheFiles() throws IOException {
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
		public int getMaxMapAttempts() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public int getMaxReduceAttempts() {
			// TODO Auto-generated method stub
			return 0;
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
		public boolean getProfileEnabled() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String getProfileParams() {
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
		public boolean getSymlink() {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String getUser() {
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

		@Override
		public float getProgress() {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public IntegerRanges getProfileTaskRange(boolean arg0) {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean getTaskCleanupNeeded() {
			// TODO Auto-generated method stub
			return false;
		}
	}
	
	public LocalMapContext getContext() {
		return new LocalMapContext();
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
