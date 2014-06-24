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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;

public class KVReplicatorHFile extends Configured implements Tool{

    public static String TABLE_NAME = "table-name";
    public static String FAMILY_NAME = "family-name";
    public static String FLUSH_SIZE = "flush-size";
    public static String REPLACE_FROM = "replace-from";
    public static String REPLACE_TO = "replace-to";
    public static String START_INDEX = "start-index";
    public static String END_INDEX = "end-index";

    
    public static class KVHFileWriterMapper extends Mapper<Text, BytesWritable, ImmutableBytesWritable, KeyValue>{
        
        byte[] familyName = null;
        byte[] qualifier = new byte[]{0};

        String replaceFrom =  "";
        String replaceTo = "";
        int startIndex = Integer.MIN_VALUE;
        int endIndex = Integer.MIN_VALUE;
        boolean hasCopy = false;
        
        ImmutableBytesWritable hKey = new ImmutableBytesWritable();
        
        @Override
        protected void setup(Context context)throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            familyName = conf.get(FAMILY_NAME).getBytes();

            replaceFrom =  conf.get(REPLACE_FROM);
            replaceTo = conf.get(REPLACE_TO);
            startIndex = conf.getInt(START_INDEX, Integer.MIN_VALUE);
            endIndex = conf.getInt(END_INDEX, Integer.MIN_VALUE);

            hasCopy =  ( null == replaceFrom ) ? false : replaceFrom.trim().length() > 0;
        }

        
        @Override
        protected void map(Text key, BytesWritable value,Context context) throws IOException, InterruptedException{
            
            String rowKey = key.toString();

            byte[] rowData = new byte[value.getLength()];
            System.arraycopy(value.getBytes(), 0, rowData, 0, value.getLength());
            
            if ( hasCopy ) {
                
            	String keyStr = rowKey.replace(replaceFrom, replaceTo);
                int index = keyStr.indexOf("[n]");
                if ( index == -1 )return;
                
                String firstPart = keyStr.substring(0, index);
                String lastPart = keyStr.substring(index + 3);
                String newKey = null;
                
                for ( int i= startIndex ; i<endIndex ; i++) {
                    newKey = firstPart + Integer.toString(i) + lastPart;
                    hKey.set(newKey.getBytes());
                    KeyValue kv = new KeyValue(hKey.get(), familyName, qualifier, rowData);
                    context.write(hKey, kv);
                }
                
            } else {
                hKey.set(rowKey.getBytes());
                KeyValue kv = new KeyValue(hKey.get(), familyName, qualifier, rowData);
                context.write(hKey, kv);
            }
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {

        int seq = 0;
        String inputFile = ( args.length > seq ) ? args[seq] : "";
        seq++;
        
        String hfileOutputFile = ( args.length > seq ) ? args[seq] : "";
        seq++;
        
        String tableName = ( args.length > seq ) ? args[seq] : "";
        seq++;
        
        String familyName = ( args.length > seq ) ? args[seq] : "1";
        seq++;
        
        String replaceFrom = ( args.length > seq ) ? args[seq] : "";
        seq++;
        
        String replaceTo = ( args.length > seq ) ? args[seq] : "";
        seq++;
        
        String startIndex = ( args.length > seq ) ? args[seq] : "";
        seq++;
        
        String endIndex = ( args.length > seq ) ? args[seq] : "";
        seq++;

        if (null == inputFile || inputFile.trim().isEmpty()) {
            String err = KVReplicatorHFile.class + " > Please enter input file path.";
            System.err.println(err);
            throw new IOException(err);
        }


        Configuration conf = HBaseConfiguration.create();
        conf.set(TABLE_NAME, tableName);
        conf.set(FAMILY_NAME, familyName);
        conf.set(REPLACE_FROM, replaceFrom);
        conf.set(REPLACE_TO, replaceTo);
        conf.set(START_INDEX, startIndex);
        conf.set(END_INDEX, endIndex);
        
        try {
            List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
            HColumnDescriptor cols = new HColumnDescriptor(familyName.getBytes());
            colFamilies.add(cols);
            HDML.create(tableName, colFamilies);
        } catch (HBaseException e) {
            e.printStackTrace();
        }
        
        Job job = Job.getInstance(conf, "KVReplicatorHBase - creating HFile");

        job.setJarByClass(KVReplicatorHFile.class);
        job.setMapperClass(KVHFileWriterMapper.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(inputFile.trim()));
        FileOutputFormat.setOutputPath(job,new Path (hfileOutputFile.trim()));
        
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        
        HTable hTable = new HTable(conf, tableName);
        HFileOutputFormat.configureIncrementalLoad(job, hTable);
        boolean result = job.waitForCompletion(true);
        
        return (result ? 0 : 1);
    }
    
    public static void main(String[] args) throws Exception {

        if(args.length < 2){
            String err = "\nUsage : "+ KVReplicatorHFile.class +" <<Input File Path>> <<HFile output path>> <<Output Table Name>> <<Family Name>> <<replace from>> <<replace to[n]>> <<start n>> <<end n>>";
            IdSearchLog.l.fatal(err);
            System.exit(1);
        }

        int exitCode = ToolRunner.run(HBaseConfiguration.create(),new KVReplicatorHFile(), args);
        System.exit(exitCode);
    }
}

