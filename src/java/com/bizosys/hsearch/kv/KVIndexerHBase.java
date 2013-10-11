package com.bizosys.hsearch.kv;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.kv.impl.FieldMapping;
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

    public void execute( String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
    	execute(new KVMapperHBase(), new KVReducer(), args);
    }
	
    @SuppressWarnings({ "deprecation", "rawtypes" })
	public void execute(KVMapperHBase map, KVReducer reduce, String[] args) throws IOException, InterruptedException, ClassNotFoundException, ParseException {
 
    	if(args.length < 2){
            System.out.println("Please enter valid number of arguments.");
            System.out.println("Usage : KVIndexer <<Input Table>> <<XML File Configuration>>");
            System.exit(1);
        }
    	
    	String inputTable = args[0];

    	if (null == inputTable || inputTable.trim().isEmpty()) {
            System.out.println("Please enter proper table");
            System.exit(1);
        }
 
    	String schemaPath = args[1];
    	
		Configuration conf = HBaseConfiguration.create();

		StringBuilder sb = new StringBuilder(8192);
		Path hadoopPath = new Path(schemaPath);
		FileSystem fs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
		String line = null;
		while((line = br.readLine())!=null) {
			sb.append(line);
		}

		br.close();
		FieldMapping fm = new FieldMapping();
		fm.parseXMLString(sb.toString());    	

    	//create table in hbase
		HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
		String outputTableName = fm.tableName;
    	if ( !admin.tableExists(outputTableName))
    		createTable(outputTableName, fm.familyName);

		conf.set(XML_FILE_PATH, schemaPath);
		conf.set(TABLE_NAME, outputTableName);
		
		Job job = new Job(conf,"KVIndexerHBase");
		job.setJarByClass(KVIndexerHBase.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		scan = scan.addFamily(fm.familyName.getBytes());
		
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
			inputTable,        // input table
			scan,               // Scan instance to control CF and attribute selection
			map.getClass(),     // mapper class
			Text.class,         // mapper output key
			Text.class,  // mapper output value
			job);
		
		TableMapReduceUtil.initTableReducerJob(
			outputTableName,        // output table
			reduce.getClass(),    // reducer class
			job);
		job.setNumReduceTasks(1);   // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
    }
    
    public void createTable(final String tableName, final String family){
		try {
			List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
			HColumnDescriptor cols = new HColumnDescriptor(family.getBytes());
			colFamilies.add(cols);
			HDML.create(tableName, colFamilies);
		} catch (HBaseException e) {
			e.printStackTrace();
		}
    }
    	    
    
    public static void main(String[] args) throws Exception {
		new KVIndexerHBase().execute(new KVMapperHBase(), new KVReducer(), args);
	}

}
