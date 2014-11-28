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
package com.bizosys.hsearch.kv.indexer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.indexing.KVKeyGenerator.KVKeyGeneratorMapperFile;
import com.bizosys.hsearch.kv.indexing.KVKeyGenerator.KVKeyGeneratorMapperHBase;
import com.bizosys.hsearch.kv.indexing.KVKeyGenerator.KVKeyGeneratorReducerFile;
import com.bizosys.hsearch.kv.indexing.KVKeyGenerator.KVKeyGeneratorReducerHBase;

/**
 * This is the base class used for indexing.
 * @author shubhendu
 *
 */
public class KVIndexer {

	public static final int SF2HB = 0;
	public static final int SF2HF = 1;
	public static final int SF2MF = 2;
	public static final int HB2HB = 6;
	public static final int HB2HF = 7;
	public static final int HB2MF = 8;
	public static final int IMF2HF= 9;
	private static final String INPUTWITH_KEY = "inputwithKey";
	public static Map<String, Integer> JobTypeMapping = new HashMap<String, Integer>();
	static {
		JobTypeMapping.put("SF2HB", SF2HB);
		JobTypeMapping.put("SF2HF", SF2HF);
		JobTypeMapping.put("SF2MF", SF2MF);
		JobTypeMapping.put("HB2HB", HB2HB);
		JobTypeMapping.put("HB2HF", HB2HF);
		JobTypeMapping.put("HB2MF", HB2MF);
		JobTypeMapping.put("IMF2HF", IMF2HF);
	}


	public static String SCANNER_CACHE_SIZE = "SCANNER_CACHE_SIZE";
	public static String XML_FILE_PATH = "CONFIG_XMLFILE_LOCATION";
	public static String INPUT_SOURCE = "input-source";
	public static String OUTPUT_FOLDER = "output-filepath";
	public static final String MERGEKEY_ROW = "--HSEARCH_PARTITION_KEYS--";
	public static String SKIP_HEADER = "false";	
	public static final String INCREMENTAL_ROW = "auto";
	public static char FIELD_SEPARATOR = '|';
	public static byte[] FAM_NAME = "1".getBytes();
	public static byte[] COL_NAME = new byte[]{0};

	public static final String PARTITION_KEY = "hsearchPkey";
	public static final String INTERNAL_KEY = "hsearchIkey";
	public static final byte[] PARTITION_KEYB = "hsearchPkey".getBytes();
	public static final byte[] INTERNAL_KEYB = "hsearchIkey".getBytes();
	public static final String RAW_FILE_SEPATATOR = "raw-file-separator";
	
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
	
    public static void main(String[] args) throws Exception{
    	new KVIndexer().execute(args);
    }
    
    /**
     * Given a indexing parameters it starts a indexing.
     * Different indexing type are:
     * SF2HB = Simple File(csv,tsv) to hbase directly.
     * SF2HF = Simple File(csv,tsv) to HFile, which can be loaded to Hbase using LoadIncrementalHfiles. class from hbase.
     * SF2MF = Simple File(csv,tsv) to MapFile (key as {@link Text} and value as {@link BytesWritable})
     * MF2HB = Map File(key and value as csv,tsv) to hbase.
     * MF2HF = Map File(key and value as csv,tsv) to HFile, which can be loaded to Hbase using LoadIncrementalHfiles. class from hbase.
     * MF2MF = Map File(key and value as csv,tsv) to MapFile(key as {@link Text} and value as {@link BytesWritable})
     * HB2HB = Hbase to Hbase
     * HB2HF = Hbase to HFile which can be loaded to Hbase using LoadIncrementalHfiles. class from hbase.
     * HB2MF = Hbase to MapFile(key as {@link Text} and value as {@link BytesWritable})
     * @param args
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    public void execute(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	
    	if(args.length < 7){
            String err = "Usage : " + KVIndexer.class + " <<Job Type(SF2HB|SF2HF|SF2MF...)>> <<Input Source>> <<Output Sink>> <<XML File Configuration>> <<Skip Header(true|false)>> <<Run KeyGeneration Job>> <<Number Of reducer>> <<Speculative Execution>> <<scanner-cache-size>> <<filter>>";
            IdSearchLog.l.fatal(err);
            System.exit(1);
        }

        String msg = this.getClass().getName() + " > Initializing indexer job.";
        IdSearchLog.l.info(msg);
        
        int seq = 0;
        int len = args.length;
        
        String jobType = ( len > seq ) ? args[seq++] : "";
    	String inputSource = ( len > seq ) ? args[seq++] : "";
    	String outputSink = ( len > seq ) ? args[seq++] : "/tmp/hsearch-index";
    	String xmlFilePath = ( len > seq ) ? args[seq++] : "";
    	String skipHeader = ( len > seq ) ? args[seq++] : "false";
    	boolean runKeyGenJob = ( len > seq ) ? args[seq++].trim().equalsIgnoreCase("true") : false;
    	int numberOfReducer = ( len > seq ) ? Integer.parseInt(args[seq++].trim()) : 1;
    	boolean speculativeExecution = ( len > seq ) ? args[seq++].trim().equalsIgnoreCase("true") : true;
    	int scannerCacheSize = ( len > seq ) ? Integer.parseInt(args[seq++].trim()) : 300;
    	String filter = ( len > seq ) ? args[seq++] : "";
    	
    	if(isEmpty(jobType)){
    		String err = this.getClass().getName() + " > Please enter Job type as one of these :\n SF2HB|SF2HF|SF2MF|MF2HB|MF2HF|MF2MF|HB2HB|HB2HF|HB2MF|IMF2HF";
            System.err.println(err);
            throw new IOException(err);
    	}
    	
    	if (isEmpty(inputSource)) {
            String err = this.getClass().getName() + " > Please enter input file path.";
            System.err.println(err);
            throw new IOException(err);
        }
    	
    	
		Configuration conf = HBaseConfiguration.create();

		FieldMapping fm = createFieldMapping(conf, xmlFilePath, new StringBuilder());
       	outputSink = outputSink.charAt(outputSink.length() - 1) == '/' ? outputSink : outputSink + "/";
		outputSink = outputSink + fm.tableName;
		
		createHBaseTable(fm);
		
		KVIndexer.FAM_NAME = fm.familyName.getBytes();
		KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;

		conf.set(XML_FILE_PATH, xmlFilePath);
		conf.set(OUTPUT_FOLDER, outputSink);
		conf.set(SKIP_HEADER, skipHeader);
		conf.set(RAW_FILE_SEPATATOR, String.valueOf(fm.fieldSeparator));
		
		Job job = Job.getInstance(conf, "com.bizosys.hsearch.kv.indexing.KVIndexer type : " + jobType + "\n" + inputSource + "\n" + outputSink );
		job.setJarByClass(this.getClass());
    	job.setNumReduceTasks(numberOfReducer);
        
		Integer jobTypeI = JobTypeMapping.get(jobType);
		if(jobTypeI == null)
			throw new IOException("Invalid Jobtype " + jobType);
		
		/**
		 *  if internal keyIndex is given then generate the keys first and then do indexing 
		 *  else just run indexer by creating keys from hbase 
		 */
		boolean keyGenjobStatus = false;
		if(-1 != fm.internalKey && runKeyGenJob){
			
        	Configuration keyGenConf = HBaseConfiguration.create();
        	keyGenConf.set(INPUT_SOURCE, inputSource);
        	keyGenConf.set(XML_FILE_PATH, xmlFilePath);
        	keyGenConf.set(OUTPUT_FOLDER, outputSink);
        	keyGenConf.set(SKIP_HEADER, skipHeader);

        	Job keyGenJob = Job.getInstance(keyGenConf, "Creating Keys KVKeyGenerator for " + inputSource);

			switch (jobTypeI) {
			case SF2HB:
			case SF2HF:
			case SF2MF:{
				
		        FileInputFormat.addInputPath(keyGenJob, new Path(inputSource));

		        keyGenJob.setMapperClass(KVKeyGeneratorMapperFile.class);
		        keyGenJob.setInputFormatClass(TextInputFormat.class);
		        keyGenJob.setMapOutputKeyClass(Text.class);
		        keyGenJob.setMapOutputValueClass(Text.class);
		        
		        keyGenJob.setReducerClass(KVKeyGeneratorReducerFile.class);
		        keyGenJob.setNumReduceTasks(numberOfReducer);
		        keyGenJob.setOutputKeyClass(NullWritable.class);
		        keyGenJob.setOutputValueClass(Text.class);
		        
		        inputSource = outputSink + "_" + INPUTWITH_KEY;
		        Path intermediatePath = new Path(inputSource);
		        System.out.println("Final input path " + inputSource);
		        FileOutputFormat.setOutputPath(keyGenJob, intermediatePath);
	            
		        keyGenjobStatus = keyGenJob.waitForCompletion(true);
				if(!keyGenjobStatus){
					throw new IOException("Error in running Job for Key Generation");
				}

				break;
			}
			case HB2HB:
			case HB2HF:
			case HB2MF:{
				
				Scan scan = new Scan();
				scan.setCaching(scannerCacheSize);
				scan.setCacheBlocks(false);
				
				byte[] family = fm.familyName.getBytes();
				for(String name : fm.nameWithField.keySet()){
					
					Field fld = fm.nameWithField.get(name);
					if(!fld.isMergedKey) continue;
					scan.addColumn(family, fld.sourceName.trim().getBytes());
				}
				
				TableMapReduceUtil.initTableMapperJob(
					inputSource,        // input table
					scan,               // Scan instance to control CF and attribute selection
					KVKeyGeneratorMapperHBase.class,     // mapper class
					Text.class,         // mapper output key
					ImmutableBytesWritable.class,  // mapper output value
					keyGenJob);
				
				TableMapReduceUtil.initTableReducerJob(
					inputSource,        // output table
					KVKeyGeneratorReducerHBase.class,    // reducer class
					keyGenJob);

		        keyGenjobStatus = keyGenJob.waitForCompletion(true);
				if(!keyGenjobStatus){
					throw new IOException("Error in running Job for Key Generation");
				}
				break;
			}
			default:
				break;
			}
		}
        /*
         * Run job based on job type eg. SF2HB,SF2MF,SF2HF etc.
         */
		System.out.println("Sending path " + inputSource);
        runJob(jobTypeI, job, fm, inputSource, outputSink, scannerCacheSize,filter);
    }

	private static int runJob(int jobTypeI, Job job, FieldMapping fm, String input, String output, 
		int scannerCacheSize, String filter) throws IOException, InterruptedException, ClassNotFoundException{

		int jobStatus = -1;
		
		switch (jobTypeI) {
		case SF2HB:{
			
	        IdSearchLog.l.info("Starting Job for SF2HB input field separator " 
	        			+ KVIndexer.FIELD_SEPARATOR +" using hbase table : " 
	        			+ fm.tableName  + " and output folder " + output);

	        FileInputFormat.addInputPath(job, new Path(input));

	        job.setMapperClass(KVMapperFile.class);
			job.setInputFormatClass(TextInputFormat.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(BytesWritable.class);
	        
	        job.setReducerClass(KVReducerHBase.class);
	        TableMapReduceUtil.initTableReducerJob(fm.tableName, KVReducerHBase.class, job);
	        jobStatus = job.waitForCompletion(true) ? 0 : 1;
			return jobStatus;
		}
		case SF2MF:{

			IdSearchLog.l.info("Starting Job for SF2MF input field separator " 
										+ KVIndexer.FIELD_SEPARATOR 
										+" using hbase table : " + fm.tableName  
										+ " and output folder " + output);

	        FileInputFormat.addInputPath(job, new Path(input));

        	job.setMapperClass(KVMapperFile.class);
			job.setInputFormatClass(TextInputFormat.class);
	        job.setMapOutputKeyClass(Text.class);
	        job.setMapOutputValueClass(BytesWritable.class);
	        
        	job.setReducerClass(KVReducerMapFile.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            LazyOutputFormat.setOutputFormatClass(job, NullOutputFormat.class);

	        jobStatus = job.waitForCompletion(true) ? 0 : 1;
			return jobStatus;
			
		}
		case SF2HF:{

			/*
			 * First creates map file and then convert to hfile.
			 * create intermediate dir for map file output
			 * 
			 */
			
			String intermediateFolder = output + "_intermediate";
			Path intermediateOutpurDir = new Path(intermediateFolder); 
			
			IdSearchLog.l.info("Starting Job for SF2HF input field separator " + 
							KVIndexer.FIELD_SEPARATOR +" using hbase table : " + fm.tableName  
							+ " and intremediate output folder " + intermediateFolder 
							+ " final output dir " + output);
			
			//reset the output folder to intermediate folder
			Configuration conf = job.getConfiguration();
			conf.set(OUTPUT_FOLDER, intermediateFolder);
			int jobT = JobTypeMapping.get("SF2MF");
			jobStatus = runJob(jobT, job, fm, input, intermediateFolder,scannerCacheSize,filter);
            
            if(jobStatus == 0){
            	
            	Configuration hfileConf = HBaseConfiguration.create();
            	hfileConf.set(XML_FILE_PATH, conf.get(XML_FILE_PATH));
            	Job hfileJob = Job.getInstance(hfileConf, "Creating Hfile");
            	String dataInputPath = intermediateFolder + "/" + MapFile.DATA_FILE_NAME;
            	jobT = JobTypeMapping.get("IMF2HF");
            	jobStatus = runJob(jobT, hfileJob, fm, dataInputPath, output,scannerCacheSize,filter);
            }
            
            //delete intermediate dir
            FileSystem.get(conf).delete(intermediateOutpurDir, true);
            //delete the empty _SUCCESS folder
            FileSystem.get(conf).delete(new Path(output, "_SUCCESS"), true);
            	
            return jobStatus;
		}
		case HB2HB:{

			if ( fm.tableName.equals(input)) {
				throw new IOException("Input table and index table can not be same");
			}
			
			Scan scan = new Scan();
			scan.setCaching(scannerCacheSize);
			scan.setCacheBlocks(false);
			scan.addFamily(fm.familyName.getBytes());
			if ( null != filter) {
				if ( filter.trim().length() > 0 ) {
					int index = filter.indexOf('=');
					scan.setFilter(new SingleColumnValueFilter(fm.familyName.getBytes(), 
						filter.substring(0, index).getBytes(), CompareOp.EQUAL, filter.substring(index+1).getBytes()));
				}
			}
			
			
			TableMapReduceUtil.initTableMapperJob(
				input,        // input table
				scan,               // Scan instance to control CF and attribute selection
				KVMapperHBase.class,     // mapper class
				Text.class,         // mapper output key
				BytesWritable.class,  // mapper output value
				job);
			
			TableMapReduceUtil.initTableReducerJob(
				fm.tableName,        // output table
				KVReducerHBase.class,    // reducer class
				job);

	        jobStatus = job.waitForCompletion(true) ? 0 : 1;
			return jobStatus;
			
		}
		case HB2HF:{
			
			String intermediateFolder = output + "_intermediate";
			Path intermediateOutpurDir = new Path(intermediateFolder); 
			
			IdSearchLog.l.info("Starting Job for HB2HF input field separator " + 
							KVIndexer.FIELD_SEPARATOR +" using hbase table : " + fm.tableName  
							+ " and intremediate output folder " + intermediateFolder 
							+ " final output dir " + output);
			
			//reset the output folder to intermediate folder
			Configuration conf = job.getConfiguration();
			conf.set(OUTPUT_FOLDER, intermediateFolder);
			int jobT = JobTypeMapping.get("HB2MF");
			jobStatus = runJob(jobT, job, fm, input, intermediateFolder,scannerCacheSize,filter);
            
            if(jobStatus == 0){
            	
            	Configuration hfileConf = HBaseConfiguration.create();
            	hfileConf.set(XML_FILE_PATH, conf.get(XML_FILE_PATH));
            	Job hfileJob = Job.getInstance(hfileConf, "Creating Hfile");
            	String dataInputPath = intermediateFolder + "/" + MapFile.DATA_FILE_NAME;
            	jobT = JobTypeMapping.get("IMF2HF");
            	jobStatus = runJob(jobT, hfileJob, fm, dataInputPath, output,scannerCacheSize,filter);
            }
            
            //delete intermediate dir
            FileSystem.get(conf).delete(intermediateOutpurDir, true);
            //delete the empty _SUCCESS folder
            FileSystem.get(conf).delete(new Path(output, "_SUCCESS"), true);
            	
            return jobStatus;
		}
		case HB2MF:{

			if ( fm.tableName.equals(input)) {
				throw new IOException("Input table and index table can not be same");
			}
			
			Scan scan = new Scan();
			scan.setCaching(scannerCacheSize);
			scan.setCacheBlocks(false);
			scan.addFamily(fm.familyName.getBytes());
			
			if ( null != filter) {
				if ( filter.trim().length() > 0 ) {
					int index = filter.indexOf('=');
					scan.setFilter(new SingleColumnValueFilter(fm.familyName.getBytes(), 
						filter.substring(0, index).getBytes(), CompareOp.EQUAL, filter.substring(index+1).getBytes()));
				}
			}
			
			TableMapReduceUtil.initTableMapperJob(
				input,        // input table
				scan,               // Scan instance to control CF and attribute selection
				KVMapperHBase.class,     // mapper class
				Text.class,         // mapper output key
				BytesWritable.class,  // mapper output value
				job);

			job.setReducerClass(KVReducerMapFile.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            LazyOutputFormat.setOutputFormatClass(job, NullOutputFormat.class);

	        jobStatus = job.waitForCompletion(true) ? 0 : 1;
			return jobStatus;
		}
		case IMF2HF:{
			
			Path finalOutputDir = new Path(output);
            job.setJarByClass(KVIndexer.class);
            job.setMapperClass(KVMapperHFile.class);

            job.setInputFormatClass(SequenceFileInputFormat.class);
            SequenceFileInputFormat.addInputPath(job, new Path(input));
            FileOutputFormat.setOutputPath(job, finalOutputDir);
            
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            
            HTable hTable = new HTable(job.getConfiguration(), fm.tableName);
            HFileOutputFormat.configureIncrementalLoad(job, hTable);
            
			jobStatus = job.waitForCompletion(true) ? 0 : 1;
			return jobStatus;
		}

		default:
			throw new IOException("Invalid Jobtype " + jobTypeI);
		}
	}
	
	public static boolean isEmpty(String data){
		return (null == data) ? true : 0 == data.trim().length(); 
	}

	private void createHBaseTable(FieldMapping fm) {
		try {
            List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
            HColumnDescriptor cols = new HColumnDescriptor(fm.familyName.getBytes());
            colFamilies.add(cols);
            HDML.create(fm.tableName, colFamilies);
        } catch (HBaseException e) {
            e.printStackTrace();
        }
	}

	public static FieldMapping createFieldMapping(Configuration conf, String path,
			StringBuilder sb) throws IOException {
		try {
			FieldMapping fm =null;
			BufferedReader br = null;
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			if ( fs.exists(hadoopPath) ) {
				br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while((line = br.readLine())!=null) {
					sb.append(line);
				}
				fm = new FieldMapping();
				fm.parseXMLString(sb.toString());
			} else {
				fm = FieldMapping.getInstance(path);
			}
			IdSearchLog.l.debug("Field mapping instance create for " + path);
			return fm;
			

		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(fex);
		} catch (ParseException pex) {
			System.err.println("Cannot Parse File " + path);
			throw new IOException(pex);
		} catch (Exception pex) {
			System.err.println("Error : " + path);
			throw new IOException(pex);
		}
	}
}	
