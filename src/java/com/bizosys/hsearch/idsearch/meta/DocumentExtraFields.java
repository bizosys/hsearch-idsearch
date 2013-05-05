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
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;

public class DocumentExtraFields {
	
	Map<String, String> values = null;
	
	public DocumentExtraFields(Map<String, String> values) {
		this.values = values;
	}
	
	public byte[] toBytes() throws IOException{
		Cell2<String, String> others = new Cell2<String, String>(
			SortedBytesString.getInstance(), SortedBytesString.getInstance());
		for (String key: values.keySet()) {
			others.add(key, values.get(key));
		}
		others.sort(new CellComparator.StringComparator<String>());
		return others.toBytesOnSortedData();
	}
	
	public static DocumentExtraFields build(byte[] data) throws IOException {
		Map<String, String> values = new HashMap<String, String>();
		return build(data, values);
	}
	
	public static DocumentExtraFields build(byte[] data, Map<String, String> values) throws IOException {
		Cell2<String, String> cell = new Cell2<String, String>
				(SortedBytesString.getInstance(), SortedBytesString.getInstance(), data);
		
		cell.populate(values);
		return new DocumentExtraFields(values);
	}

	public String toString() {
		return this.values.toString();
	}
	
	public static void main(String[] args) throws IOException {
		Map<String, String> others = new HashMap<String, String>();
		others.put("age" , "23");
		others.put("sex" , "male");
		others.put("location" , "bangalore");
		
		
		DocumentExtraFields serWeight = new DocumentExtraFields(others);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		Map<String, String> foundValues = new HashMap<String, String>();
		for ( int i=0; i<1000000; i++) {
			foundValues.clear();
			DocumentExtraFields.build(ser, foundValues);
		}
		long end = System.currentTimeMillis();
		System.out.println (DocumentExtraFields.build(ser).values.keySet().toString() + "   in " + (end - start) );
	}
}
