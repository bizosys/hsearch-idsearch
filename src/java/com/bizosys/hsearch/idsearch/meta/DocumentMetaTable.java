/*
* Copyright 2010 Bizosys Technologies Limited
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

package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.treetable.Cell2;

public class DocumentMetaTable extends Cell2<Integer, byte[]> {
	
	public DocumentMetaTable() {
		super(SortedBytesInteger.getInstance(), SortedBytesArray.getInstance());
	}
	
	public DocumentMetaTable(byte[] data) throws Exception  {
		super(SortedBytesInteger.getInstance(), SortedBytesArray.getInstance(), data);
	}

	public void add(Integer docId, DocumentMeta aRow) throws Exception {
		super.add(docId, aRow.toBytes());
	}

	public byte[] toBytes() throws IOException {
		return super.toBytesOnSortedData();
	}
	
	
	public DocumentMeta get(Integer docId) throws Exception {
		SortedBytesArray sba = SortedBytesArray.getInstanceArr();
		sba.parse(data.data, data.offset, data.length);
		
		Reference keysRef = sba.getValueAtReference(0);
		if ( null == keysRef ) return null;
		
		Reference valRef = sba.getValueAtReference(1);
		if ( null == valRef ) return null;
		
		int size = k1Sorter.parse(data.data, keysRef.offset, keysRef.length).getSize();
		for ( int i=0; i<size; i++) {
			int docIdRaw = docId.intValue();

			if ( docIdRaw ==  k1Sorter.getValueAt(i)) {

				byte[] rowB = vSorter.parse(this.data.data, valRef.offset, valRef.length).getValueAt(i);
				return new DocumentMeta(rowB);
			}
		}
		return null;
	}
	
	public void put(Integer rowId, DocumentMeta row) throws Exception {
		byte[] rowB = row.toBytes();
		super.add(rowId, rowB);
	}
	
}
