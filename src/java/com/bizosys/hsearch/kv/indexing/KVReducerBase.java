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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.iq80.snappy.Snappy;

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.byteutils.SortedBytesBitsetCompressed;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVIndex;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldBoolean;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldByte;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldDouble;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldFloat;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldInteger;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldLong;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldShort;
import com.bizosys.hsearch.kv.impl.bytescooker.IndexFieldString;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVReducerBase {

	public byte[] cookBytes(StringBuilder key, Iterable<Text> values, byte[] existingData,Field fld, char dataTypeChar) throws IOException {
		
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
				finalData = IndexFieldString.cook(values, existingData, repeatable, compressed);
				break;

			case 'e':
				
				/**
				 * Skip multi phrases which are only sighted once.
				 */
				int keyLen = key.length();
				boolean skipSingle = false;
				if ( keyLen > 1) {
					skipSingle = ( key.charAt(keyLen - 1) == '*');
					if ( skipSingle ) key = key.delete(keyLen - 2, keyLen);
				}
				 
				finalData = ( repeatable ) ? 
					indexTextBitset(skipSingle, existingData, values, analyzed, fieldName, compressed) : 
					indexTextSet(skipSingle, existingData,  values, analyzed, fieldName);
				break;

			case 'i':
				finalData = IndexFieldInteger.cook(values, existingData, repeatable, compressed);
				break;

			case 'f':
				finalData = IndexFieldFloat.cook(values, existingData, repeatable, compressed);
				break;

			case 'd':
				finalData = IndexFieldDouble.cook(values, existingData, repeatable, compressed);
				break;

			case 'l':
				finalData = IndexFieldLong.cook(values, existingData, repeatable, compressed);
				break;

			case 's':
				finalData = IndexFieldShort.cook(values, existingData, repeatable, compressed);
				break;

			case 'b':
				finalData = IndexFieldBoolean.cook(values, existingData, repeatable, compressed);
				break;

			case 'c':
				finalData = IndexFieldByte.cook(values, existingData, repeatable, compressed);
				break;

			default:
			{
				List<String> mergeKeys = new ArrayList<String>();
				for (Text mergeKey : values) {
					mergeKeys.add(mergeKey.toString());
				}
				finalData = SortedBytesString.getInstance().toBytes(mergeKeys);
				break;
			}
		}
		return finalData;
	}
	
	Set<Integer> records = new HashSet<Integer>(3);
	public byte[] indexTextSet(boolean skipSingle, byte[] existingData, Iterable<Text> values,   
		boolean isAnalyzed, String fieldName) throws IOException {
		
		byte[] finalData = null;
		int containerKey = 0;
		int containervalue = 0;
		String[] resultValue = new String[2];
		HSearchTableKVIndex table = new HSearchTableKVIndex();
		
		if ( null != existingData) {
			throw new IOException("Append is not supported for the non repetable analyzed fields");
		}
		
		String line = null;
		
		int docType = 1;
		int fieldType = 1;
		String metaDoc = "-";
		boolean flag = true;
		
		records.clear();
		for (Text text : values) {

			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();

			LineReaderUtil.fastSplit(resultValue, line, KVIndexer.FIELD_SEPARATOR);

			containerKey = Integer.parseInt(resultValue[0]);
			if(null == resultValue[1]) continue;
			containervalue = Integer.parseInt(resultValue[1]);
			table.put(docType, fieldType, metaDoc, containervalue, containerKey, flag);
			
			if ( skipSingle) {
				if ( records.size() < 2 ) records.add(containerKey);
			}
			
		}
		
		if ( skipSingle && records.size() < 2) return null;

		finalData = table.toBytes();		
		return finalData;
	}
	
	public byte[] indexTextBitset(boolean skipSingle, byte[] existingData, 
		Iterable<Text> values, boolean isAnalyzed, String fieldName, boolean isCompressed) throws IOException {
		
		byte[] finalData = null;
		int containerKey = 0;
		
		BitSetWrapper foundIds = null;
		int existingDataLen = ( null == existingData) ? 0 : existingData.length;
		if ( existingDataLen > 0 ) {
			byte[] uncompressedData = existingData;
			if ( isCompressed ) {
				uncompressedData = Snappy.uncompress(existingData, 0 , existingDataLen);
			} 
			foundIds = SortedBytesBitset.getInstanceBitset().bytesToBitSet(uncompressedData, 0, uncompressedData.length);
		} else {
			foundIds = new BitSetWrapper();
		}
		
		for (Text text : values) {
			if ( null == text) continue;
			containerKey = Integer.parseInt(text.toString());
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
