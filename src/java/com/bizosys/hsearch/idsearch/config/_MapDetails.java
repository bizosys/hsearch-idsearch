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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class _MapDetails {
	
	Map<String, String> nameLines = new HashMap<String, String>();
	public _MapDetails (byte[] data) throws IOException  {
		
		Cell2<String, String> codes = new Cell2<String, String>(
			SortedBytesString.getInstance(), SortedBytesString.getInstance(), data);
		
		for (CellKeyValue<String, String> ckv: codes.getMap()) {
			nameLines.put(ckv.key, ckv.value);
		}
	}
	
	
	public String getCode(String name) throws IOException {
		if ( ! this.nameLines.containsKey(name)) {
			throw new IOException(
				"Name is not available. Current data size :" + this.nameLines.size());
		}
		return this.nameLines.get(name);
	}
	
	public boolean hasCode(String name) throws IOException {
		return this.nameLines.containsKey(name);
	}
	
	public static _MapDetails.Builder builder() {
		return new _MapDetails.Builder();
	}	
	
	public static class Builder {
		
		Cell2<String, String> nameDetails = null;
		public Builder() {
			nameDetails = new Cell2<String, String>(
				SortedBytesString.getInstance(), SortedBytesString.getInstance());
		}		
		
		public byte[] toBytes() throws IOException {
			nameDetails.sort(new CellComparator.StringComparator<String>());
			return nameDetails.toBytesOnSortedData();
		}		
		
		public Builder add(Map<String, String> codes) {
			for (String name : codes.keySet()) {
				nameDetails.add(name, codes.get(name));
			}
			return this;
		}
		
		public Builder add(String name, String id) {
			nameDetails.add(name, id);
			return this;
		}

	}
	
	  public static List<String> fastSplit(final String text, char separator) {
		  if (null == text) return null;

		  final List<String> result = new ArrayList<String>();
		  int index1 = 0;
		  int index2 = text.indexOf(separator);
		  String token = null;
		  while (index2 >= 0) {
			  token = text.substring(index1, index2);
			  result.add(token);
			  index1 = index2 + 1;
			  index2 = text.indexOf(separator, index1);
		  }
	            
		  if (index1 < text.length() - 1) {
			  result.add(text.substring(index1));
		  }
		  return result;
	  }	
}
