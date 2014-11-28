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

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

import com.bizosys.hsearch.byteutils.ByteUtil;
import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.byteutils.SortedBytesBitsetCompressed;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldBoolean;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldByte;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldDouble;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldFloat;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldInteger;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldLong;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldShort;
import com.bizosys.hsearch.kv.impl.bytescookernew.IndexFieldString;

public class KVReducerBase {

	public byte[] cookBytes(StructureKey structureKey, Iterable<BytesWritable> values, Field fld, char dataTypeChar) throws IOException {
		
		byte[] finalData = null;
		String fieldName = null;
		boolean compressed = false;
		boolean repeatable = false;
		boolean analyzed = false;

		if(null != fld){
			fieldName = fld.name;
			compressed = fld.isCompressed;
			repeatable = fld.isRepeatable;
			analyzed = fld.isAnalyzed;
		}
		
		switch (dataTypeChar) {

			case 't':
				finalData = IndexFieldString.cook(values, repeatable, compressed);
				break;

			case 'e':
				
				/**
				 * Skip multi phrases which are only sighted once.
				 */
				String valueField = structureKey.valueField;
				int keyLen = valueField.length();
				boolean skipSingle = false;
				if ( keyLen > 1) {
					skipSingle = ( valueField.charAt(keyLen - 1) == '*');
					if ( skipSingle ) structureKey.valueField = structureKey.valueField.substring(0,keyLen - 1);
				}
				finalData = indexTextBitset(skipSingle, values, analyzed, fieldName, compressed);
				break;

			case 'i':
				finalData = IndexFieldInteger.cook(values, repeatable, compressed);
				break;

			case 'f':
				finalData = IndexFieldFloat.cook(values, repeatable, compressed);
				break;

			case 'd':
				finalData = IndexFieldDouble.cook(values, repeatable, compressed);
				break;

			case 'l':
				finalData = IndexFieldLong.cook(values, repeatable, compressed);
				break;

			case 's':
				finalData = IndexFieldShort.cook(values, repeatable, compressed);
				break;

			case 'b':
				finalData = IndexFieldBoolean.cook(values, repeatable, compressed);
				break;

			case 'c':
				finalData = IndexFieldByte.cook(values, repeatable, compressed);
				break;

			default:
			{
				throw new IOException("Invalid data type [ " + dataTypeChar  + "]");
			}
		}
		return finalData;
	}
	
	public byte[] indexTextBitset(boolean skipSingle,  
		Iterable<BytesWritable> values, boolean isAnalyzed, String fieldName, boolean isCompressed) throws IOException {
		
		byte[] finalData = null;
		int containerKey = 0;
		
		BitSetWrapper foundIds = new BitSetWrapper();
		
		for (BytesWritable idBytes : values) {
			containerKey = ByteUtil.toInt(idBytes.getBytes(), 0);
			foundIds.set(containerKey);
		}
		
		if ( skipSingle && foundIds.cardinality() < 2) return null;
		
		if(isCompressed)
			finalData = SortedBytesBitsetCompressed.getInstanceBitset().bitSetToBytes(foundIds);
		else
			finalData = SortedBytesBitset.getInstanceBitset().bitSetToBytes(foundIds);
		
		if ( null == finalData) return finalData;
		 
		return finalData;
	}	
}
