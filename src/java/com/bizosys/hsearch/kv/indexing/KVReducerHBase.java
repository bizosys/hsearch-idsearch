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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.hbase.HReader;
import com.bizosys.hsearch.hbase.NVBytes;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.indexing.KVIndexer.KV;

public class KVReducerHBase extends TableReducer<TextPair, Text, ImmutableBytesWritable> {

	public KV onReduce(KV kv ) {
		return kv;
	}

	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;
	KVReducerBase reducerUtil = null;

	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String path = conf.get(KVIndexer.XML_FILE_PATH);
		fm = KVIndexer.createFieldMapping(conf, path, new StringBuilder());
		neededPositions = fm.sourceSeqWithField.keySet();
		KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
		
		reducerUtil = new KVReducerBase();
	}	


	@Override
	protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

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
		
		try {
		
			byte[] finalData = reducerUtil.cookBytes(rowKeyText, values, existingData, fld, dataTypeChar);
			if(null == finalData) return;

			Put put = new Put(rowKeyText.toString().getBytes());
			put.add(KVIndexer.FAM_NAME,KVIndexer.COL_NAME, finalData);

			context.write(null, put);
			
		} catch (NumberFormatException ex) {
			IdSearchLog.l.fatal("Error at index reducer", ex);
			Iterator<Text> itr = values.iterator();
			while (itr.hasNext()) {
				Text text = itr.next();
				IdSearchLog.l.fatal(text.toString());
			}
			throw new IOException("Error indexing data because " + ex.getMessage());
		}
	}
}
