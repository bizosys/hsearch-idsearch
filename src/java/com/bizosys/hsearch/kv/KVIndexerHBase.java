package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.bizosys.hsearch.kv.impl.KVMapperHBase;
import com.bizosys.hsearch.kv.impl.KVReducer;

public class KVIndexerHBase {

	public static String XML_FILE_PATH = "CONFIG_XMLFILE_LOCATION";
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

    public void execute( String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	execute(new KVMapperHBase(), new KVReducer(), args);
    }
	
    @SuppressWarnings({ "deprecation", "rawtypes" })
	public void execute(KVMapperHBase map, KVReducer reduce, String[] args) throws IOException, InterruptedException, ClassNotFoundException {
 
    	if(args.length < 3){
            System.out.println("Please enter valid number of arguments.");
            System.out.println("Usage : KVIndexer <<Input Table>> <<XML File Configuration>> <<Destination Table>>");
            System.exit(1);
        }
    	
    	String inputFile = args[0];

    	if (null == inputFile || inputFile.trim().isEmpty()) {
            System.out.println("Please enter proper path");
            System.exit(1);
        }
 
		Configuration conf = HBaseConfiguration.create();
		String tableName = args[2];
		conf.set(XML_FILE_PATH, args[1]);
		conf.set(TABLE_NAME, tableName);
		
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"ExampleSummary");
		job.setJarByClass(KVIndexerHBase.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
			tableName,        // input table
			scan,               // Scan instance to control CF and attribute selection
			map.getClass(),     // mapper class
			Text.class,         // mapper output key
			Text.class,  // mapper output value
			job);
		TableMapReduceUtil.initTableReducerJob(
			tableName,        // output table
			reduce.getClass(),    // reducer class
			job);
		job.setNumReduceTasks(1);   // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
    }
    
    public static void main(String[] args) throws Exception {
		new KVIndexerHBase().execute(new KVMapperHBase(), new KVReducer(), args);
	}

}
