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
package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class _MapCodes {
	
	Map<String, Integer> nameIds = new HashMap<String, Integer>();
	public _MapCodes (byte[] data) throws IOException  {
		
		Cell2<String, Integer> codes = new Cell2<String, Integer>(
			SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), data);
		
		for (CellKeyValue<String, Integer> ckv: codes.getMap()) {
			nameIds.put(ckv.key, ckv.value);
		}
	}
	
	
	public Integer getCode(String name) throws IOException {
		if ( ! this.nameIds.containsKey(name)) {
			throw new IOException(
					name + " field name is not available. Available field names are:" + this.nameIds.toString());
		}
		return this.nameIds.get(name);
	}
	
	public boolean hasCode(String name) throws IOException {
		return this.nameIds.containsKey(name);
	}
	
	public static _MapCodes.Builder builder() {
		return new _MapCodes.Builder();
	}	
	
	public static class Builder {
		
		Cell2<String, Integer> nameIds = null;
		public Builder() {
			nameIds = new Cell2<String, Integer>(
				SortedBytesString.getInstance(), SortedBytesInteger.getInstance());
		}		
		
		public byte[] toBytes() throws IOException {
			nameIds.sort(new CellComparator.IntegerComparator<String>());
			return nameIds.toBytesOnSortedData();
		}		
		
		public Builder add(Map<String, Integer> codes) {
			for (String name : codes.keySet()) {
				nameIds.add(name, codes.get(name));
			}
			return this;
		}
		
		public Builder add(String name, Integer id) {
			nameIds.add(name, id);
			return this;
		}

	}

}
