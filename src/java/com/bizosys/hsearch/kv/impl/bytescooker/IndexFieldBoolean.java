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
package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVBooleanInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVBoolean;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class IndexFieldBoolean {

	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook(values, null, isRepetable, isCompressed);
	}

	public static byte[] cook(Iterable<Text> values, final byte[] exstingData, 
			final boolean isRepetable, final boolean isCompressed ) throws IOException {
		
		IndexField fld = null;

		if ( isRepetable ) {
		
			fld = new IndexField() {
			
				HSearchTableKVBooleanInverted table = new HSearchTableKVBooleanInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, Boolean.parseBoolean(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}

				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSetWrapper, Boolean>() {

						@Override
						public void visit(BitSetWrapper k, Boolean v) {
							table.put(k, v);
						}
					});
				}
				
			};			
		} else {
			
			fld = new IndexField() {
			
				HSearchTableKVBoolean table = new HSearchTableKVBoolean();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Boolean.parseBoolean(val));
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					
					table.parse(data, new Cell2Visitor<Integer, Boolean>() {

						@Override
						public void visit(Integer k, Boolean v) {
							table.put(k, v);
						}
					});
				}
				
			};
		}
		
		if ( null != exstingData ) fld.append(exstingData);
		return fld.index(values);
	}

}
