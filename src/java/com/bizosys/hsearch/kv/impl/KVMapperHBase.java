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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

public class KVMapperHBase extends TableMapper<Text, Text> {
    	
	KVMapperBase kBase = null;
	String[] result = null; 
	Map<String, Field> sourceNameWithFields = null;
	Map<String, Field> nameWithFields = null;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		kBase = new KVMapperBase();
		kBase.setup(context);
		nameWithFields = kBase.fm.nameWithField;
		sourceNameWithFields = new HashMap<String, Field>();
		
		int maxSourceSeq = kBase.fm.nameWithField.size() - 1;
		
		for ( Field fld : kBase.fm.nameWithField.values()) {
			if ( fld.sourceSeq < 0) throw new IOException("Invalid Field Configuration : " + fld.name);
			if ( maxSourceSeq < fld.sourceSeq) maxSourceSeq = fld.sourceSeq;
			
			sourceNameWithFields.put(fld.sourceName, fld);
		}
		result = new String[maxSourceSeq+1];
	}

	
    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    	
    	byte[] rowId = value.getRow();
    	if ( null == rowId) return;
    	if ( 0 == rowId.length) return;
    	
    	Arrays.fill(result, "");
    	
    	String qualifier = null;
    	Field fld = null;
    	
    	Exception failure = null;
    	
    	try {
			for (KeyValue kv : value.list()) {
				byte[] qualifierB = kv.getQualifier();
				int qualifierLen = ( null == qualifierB) ? 0 : qualifierB.length;
				if ( qualifierLen == 0 ) continue;
				
				qualifier = new String(qualifierB);
				byte[] valB = kv.getValue();
				String val = null;
				fld = sourceNameWithFields.get(qualifier);
				if ( null == fld) continue;
				
				int len = ( null == valB) ? 0 : valB.length;
				if ( len > 0 ) {
					val = new String(valB);
				}
				
				result[fld.sourceSeq] = val;
			}
			
			kBase.map(result, context);

		} catch (NullPointerException e) {
			failure = e;
		} catch (Exception e) {
			failure = e;
		}
    	
    	if ( null != failure) {
			String msg = "Failed while processing qualifier [" ;
			if ( null != qualifier) msg = msg +  qualifier;
			msg = msg + "] And Field[";
			
			if ( null != fld) msg = msg +  (fld.name + "\t" + fld.getDataType());
			msg = msg +  "] And Row = [" + new String(rowId) + "]";

			throw new IOException(msg, failure);    		
    	}
    }
}
