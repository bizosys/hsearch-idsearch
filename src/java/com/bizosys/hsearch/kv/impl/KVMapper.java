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
package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
	private KVMapperBase kBase = new KVMapperBase();
	String[] result = null;
	boolean isSkipHeader = false;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String skipHeader = conf.get(KVIndexer.SKIP_HEADER);
		if ( null != skipHeader) {
			isSkipHeader = "true".equals(skipHeader);
		}
		kBase.setup(context);
	}
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	if ( isSkipHeader ) {
			isSkipHeader = false;
    		if ( 0 == key.get()) return;
    	}
    	
    	if ( null == result) {
    		ArrayList<String> resultL = new ArrayList<String>();
    		LineReaderUtil.fastSplit(resultL, value.toString(), KVIndexer.FIELD_SEPARATOR);
    		result = new String[resultL.size()];
    	}
    	Arrays.fill(result, null);

    	LineReaderUtil.fastSplit(result, value.toString(), KVIndexer.FIELD_SEPARATOR);
    	
    	kBase.map(result, context);
    	
    }
}
