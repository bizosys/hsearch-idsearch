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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.idsearch.util.MapFileUtil;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.indexing.KVIndexer.KV;

class KVReducerMapFile extends Reducer<TextPair, Text, Text, BytesWritable>{

	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = IdSearchLog.l.isInfoEnabled();
	
	public KV onReduce(KV kv ) {
		return kv;
	}
	
	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;
	KVReducerBase reducerUtil = null;
	String outputFolder = null;
	
	MapFileUtil.Writer writer = null;
	MapFileUtil.Writer metaWriter = null;
	
	@Override
	protected void setup(Context context)throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		fm = KVIndexer.createFieldMapping(conf, path, new StringBuilder());
		neededPositions = fm.sourceSeqWithField.keySet();
		KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
			
		reducerUtil = new KVReducerBase();
		outputFolder = conf.get(KVIndexer.OUTPUT_FOLDER);

		writer = new MapFileUtil.Writer();
		writer.setConfiguration(conf);
		writer.open(Text.class, ImmutableBytesWritable.class, outputFolder, CompressionType.NONE);

	}

	@Override
	protected void reduce(TextPair key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		
		long start = System.currentTimeMillis();
		String rowKeyP1 = key.getFirst().toString();
		String rowKeyP2 = key.getSecond().toString();
		int sepIndex = -1;
		String dataType = null;
		int sourceSeq = 0;
		Field fld = null;
		char dataTypeChar = '-';

		if(!rowKeyP1.equals(KVIndexer.MERGEKEY_ROW)){
			
			sepIndex = rowKeyP2.indexOf(KVIndexer.FIELD_SEPARATOR);
			dataType = rowKeyP2.substring(0, sepIndex).toLowerCase();
			sourceSeq = Integer.parseInt(rowKeyP2.substring(sepIndex + 1));
			fld = fm.sourceSeqWithField.get(sourceSeq);
			dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType);

		}
    	
		byte[] existingData = null;
		
		if ( fm.append ){
			List<NVBytes> cells = HReader.getCompleteRow(fm.tableName, rowKeyP1.getBytes());
			if ( null != cells) {
				if ( cells.size() > 0 ) {
					NVBytes nvBytes = cells.get(0);
					if ( null != nvBytes ) existingData = nvBytes.data;
				}
			}

		}
		
		StringBuilder rowKeyText = new StringBuilder(rowKeyP1);
		int size = 0;
        try {
        	
        	byte[] finalData = reducerUtil.cookBytes(rowKeyText, values, existingData, fld, dataTypeChar);
        	size = ( null == finalData ) ? 0 : finalData.length;
    		if(0 == size) return;
    		context.setStatus("Key = " + rowKeyText.toString() + " ValueSize = " + size/1000 + " KB"); 
    		writer.append(new Text(rowKeyText.toString()), new ImmutableBytesWritable(finalData));
    		
		} catch (Exception e) {
			System.err.println("Error putting data for row : " + rowKeyP1 + " and datalength : " + size + " because - " + e.getMessage());
			e.printStackTrace();
			throw new IOException("Error indexing data because " + e.getMessage());
		} finally{
			if(INFO_ENABLED){
				IdSearchLog.l.debug("Key : " + rowKeyP1 + "\tFree mem : " + Runtime.getRuntime().freeMemory()
				+ "\tMax mem : " + Runtime.getRuntime().maxMemory()
				+ "\tTotal mem : " + Runtime.getRuntime().totalMemory()
				+"\tTime taken : " + (System.currentTimeMillis() - start));
			}
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
