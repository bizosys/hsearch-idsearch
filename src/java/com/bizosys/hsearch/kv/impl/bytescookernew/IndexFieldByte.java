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
package com.bizosys.hsearch.kv.impl.bytescookernew;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;

import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVByteInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVByte;
import com.bizosys.hsearch.treetable.Cell2;

public class IndexFieldByte {
	
	public static byte[] cook(Iterable<BytesWritable> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		if ( isRepetable ) {
			final HSearchTableKVByteInverted table = new HSearchTableKVByteInverted(isCompressed);
			ComputeSerValues<Byte> compute = new ComputeSerValues<Byte>() {
				@Override
				public final void visit(final int key, final Byte val) {
					table.put(key, val);
				}
			};

			for (BytesWritable bucketBytes : values) {

				byte[] data = bucketBytes.getBytes();
				if(null == data) continue;

				compute.compute(createBlankTable(), data);			
			}

			return table.toBytes();

		} else {
			final HSearchTableKVByte table = new HSearchTableKVByte();
			ComputeSerValues<Byte> compute = new ComputeSerValues<Byte>() {
				@Override
				public final void visit(final int key, final Byte val) {
					table.put(key, val);
				}
			};

			
			for (BytesWritable bucketBytes : values) {

				byte[] data = bucketBytes.getBytes();
				if(null == data) continue;

				compute.compute(createBlankTable(), data);			
			}

			return table.toBytes();
		}
	}
	
	public static final Cell2<Integer,Byte> createBlankTable() {
		return new Cell2<Integer,Byte>(SortedBytesInteger.getInstance(),SortedBytesChar.getInstance());
	}

}
