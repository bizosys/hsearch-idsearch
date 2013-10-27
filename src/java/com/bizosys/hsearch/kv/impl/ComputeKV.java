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
import java.io.OutputStream;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesLong;
import com.bizosys.hsearch.byteutils.SortedBytesShort;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.util.HSearchLog;

public final class ComputeKV implements ICompute {
	
	public ComputeKV() {
	}
	
	public int kvType = 1;
	public boolean kvRepeatation = false;
	public boolean isCompressed = false;
	public int fieldSeq = 0;
	public int totalFields = 0;

	Cell2<Integer, Boolean> kv_boolean = null;
	Cell2<Integer, Byte> kv_byte = null;
	Cell2<Integer, Short> kv_short = null;
	Cell2<Integer, Integer> kv_integer = null;
	Cell2<Integer, Float> kv_float = null;
	Cell2<Integer, Long> kv_long = null;
	Cell2<Integer, Double> kv_double = null;
	Cell2<Integer, String> kv_string = null;
		
	public Map<Integer, Object> rowContainer = null;
	
	@Override
	public final void setCallBackType(final int callbackType) {
		this.kvType = callbackType;
	}

	/**
	 * Mapper invokes as it keeps getting columns
	 */
	public final void put(final int key, final Object value) {
		
		switch (this.kvType) {
			case Datatype.BOOLEAN:
				if ( null == kv_boolean) kv_boolean = new Cell2<Integer, Boolean>(
					SortedBytesInteger.getInstance(), SortedBytesBoolean.getInstance());
				kv_boolean.add( key, (Boolean) value);
				break;
				
			case Datatype.BYTE:
				if ( null == kv_byte) kv_byte = new Cell2<Integer, Byte>(
					SortedBytesInteger.getInstance(), SortedBytesChar.getInstance());
				kv_byte.add( key, (Byte) value);
				break;
				
			case Datatype.SHORT:
				if ( null == kv_short) kv_short = new Cell2<Integer, Short>(
					SortedBytesInteger.getInstance(), SortedBytesShort.getInstance());
				kv_short.add( key, (Short) value);
				break;
				
			case Datatype.INTEGER:
				if ( null == kv_integer) kv_integer = new Cell2<Integer, Integer>(
					SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());
				kv_integer.add( key, (Integer) value);
				break;
				
			case Datatype.FLOAT:
				if ( null == kv_float) kv_float = new Cell2<Integer, Float>(
					SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
				kv_float.add( key, (Float) value);
				break;
				
			case Datatype.LONG:
				if ( null == kv_long) kv_long = new Cell2<Integer, Long>(
					SortedBytesInteger.getInstance(), SortedBytesLong.getInstance());
				kv_long.add( key, (Long) value);
				break;
				
			case Datatype.DOUBLE:
				if ( null == kv_double) kv_double = new Cell2<Integer, Double>(
					SortedBytesInteger.getInstance(), SortedBytesDouble.getInstance());
				kv_double.add( key, (Double) value);
				break;
				
			case Datatype.STRING:
				if ( null == kv_string) kv_string = new Cell2<Integer, String>(
					SortedBytesInteger.getInstance(), SortedBytesString.getInstance());
				kv_string.add( key, (String) value);
				break;
				
		}
	}
	
	public final ComputeKV createNew() {
		ComputeKV kv = new ComputeKV();
		kv.kvType = this.kvType;
		return kv;
	}
	
	
	public final void clear() {
		
		switch (this.kvType) {
		
			case Datatype.BOOLEAN:
				if ( null != kv_boolean) {
					kv_boolean.sortedList.clear();
					kv_boolean.data = null;
				}
				break;
				
			case Datatype.BYTE:
				if ( null != kv_byte) {
					kv_byte.sortedList.clear();
					kv_byte.data = null;
				}
				break;
				
			case Datatype.SHORT:
				if ( null != kv_short) {
					kv_short.sortedList.clear();
					kv_short.data = null;
				}
				break;
				
			case Datatype.INTEGER:
				if ( null != kv_integer) {
					kv_integer.sortedList.clear();
					kv_integer.data = null;
				}
				break;
				
			case Datatype.FLOAT:
				if ( null != kv_float) {
					kv_float.sortedList.clear();
					kv_float.data = null;
				}
				break;
				
			case Datatype.LONG:
				if ( null != kv_long) {
					kv_long.sortedList.clear();
					kv_long.data = null;
				}
				break;
				
			case Datatype.DOUBLE:
				if ( null != kv_double) {
					kv_double.sortedList.clear();
					kv_double.data = null;
				}
				break;
				
			case Datatype.STRING:
				if ( null != kv_string) {
					kv_string.sortedList.clear();
					kv_string.data = null;
				}
				break;
							
			default:
				HSearchLog.l.error( "Unable to find the datatype" + this.kvType);
				break;
		}
		
	}	
	
	public final byte[] toBytes() throws IOException {
		
		byte[] data = null; 
		switch (this.kvType) {
		
			case Datatype.BOOLEAN:
				if ( null != kv_boolean)
					data = kv_boolean.toBytesOnSortedData();
				break;
			
			case Datatype.BYTE:
				if ( null != kv_byte)
					data = kv_byte.toBytesOnSortedData();
				break;
			
			case Datatype.SHORT:
				if ( null != kv_short)
					data = kv_short.toBytesOnSortedData();
				break;
			
			case Datatype.INTEGER:
				if ( null != kv_integer)
					data = kv_integer.toBytesOnSortedData();
				break;
			
			case Datatype.FLOAT:
				if ( null != kv_float)
					data = kv_float.toBytesOnSortedData();
				break;
			
			case Datatype.LONG:
				if ( null != kv_long)
					data = kv_long.toBytesOnSortedData();
				break;
			
			case Datatype.DOUBLE:
				if ( null != kv_double)
					data = kv_double.toBytesOnSortedData();
				break;
			
			case Datatype.STRING:
				if ( null != kv_string)
					data = kv_string.toBytesOnSortedData();
				break;
				
			default:
				return null;
		}
		return data;
	}
	
	@Override
	public final void put(final byte[] data) throws IOException {
		put(data, null);
	}
	
	public final void parse(final byte[] dataChunk) throws IOException {
		parse(dataChunk, null);
	}
	
	public final void put(final byte[] data, BitSetWrapper bitset) throws IOException {
		for (byte[] dataChunk : SortedBytesArray.getInstanceArr().parse(data).values()) {
			parse(dataChunk, bitset);
		}
	}
	
	public final void parse(final byte[] dataChunk, BitSetWrapper bitset) throws IOException {
		switch (this.kvType) {
			case Datatype.BOOLEAN:
			{
				kv_boolean = new Cell2<Integer, Boolean>(SortedBytesInteger.getInstance(), SortedBytesBoolean.getInstance(), dataChunk);
				
				ComputeKVRowVisitor<Boolean> visitor = new ComputeKVRowVisitor<Boolean>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_boolean.process(visitor);
				break;
			}
			case Datatype.BYTE:
			{
				kv_byte = new Cell2<Integer, Byte>(
						SortedBytesInteger.getInstance(), SortedBytesChar.getInstance(), dataChunk);
				
				ComputeKVRowVisitor<Byte> visitor = new ComputeKVRowVisitor<Byte>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_byte.process(visitor);
				break;
			}
			case Datatype.SHORT:
			{
				kv_short = new Cell2<Integer, Short>(
						SortedBytesInteger.getInstance(), SortedBytesShort.getInstance(), dataChunk);
				ComputeKVRowVisitor<Short> visitor = new ComputeKVRowVisitor<Short>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_short.process(visitor);
				break;
			}
			case Datatype.INTEGER:
			{
				kv_integer = new Cell2<Integer, Integer>(
						SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), dataChunk);
				ComputeKVRowVisitor<Integer> visitor = new ComputeKVRowVisitor<Integer>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_integer.process(visitor);
				break;
			}
			case Datatype.FLOAT:
			{
				kv_float = new Cell2<Integer, Float>(
						SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance(), dataChunk);
				ComputeKVRowVisitor<Float> visitor = new ComputeKVRowVisitor<Float>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_float.process(visitor);
				break;
			}
			case Datatype.LONG:
			{
				kv_long = new Cell2<Integer, Long>(
						SortedBytesInteger.getInstance(), SortedBytesLong.getInstance(), dataChunk);
				ComputeKVRowVisitor<Long> visitor = new ComputeKVRowVisitor<Long>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_long.process(visitor);
				break;
			}
			case Datatype.DOUBLE:
			{
				kv_double = new Cell2<Integer, Double>(
						SortedBytesInteger.getInstance(), SortedBytesDouble.getInstance(), dataChunk);
				ComputeKVRowVisitor<Double> visitor = new ComputeKVRowVisitor<Double>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_double.process(visitor);
				break;
			}
			case Datatype.STRING:
			{
				kv_string = new Cell2<Integer, String>(
						SortedBytesInteger.getInstance(), SortedBytesString.getInstance(), dataChunk);
				ComputeKVRowVisitor<String> visitor = new ComputeKVRowVisitor<String>(rowContainer);
				visitor.setMatchingIds(bitset);
				kv_string.process(visitor);
				break;
			}
			default: break;
		}
	}

	@Override
	public final void setStreamWriter(final OutputStream out) {
	}
	
	@Override
	public final void onComplete() {
		
	}
}
