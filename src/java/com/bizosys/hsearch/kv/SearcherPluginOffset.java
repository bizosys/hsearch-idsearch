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

package com.bizosys.hsearch.kv;

import java.util.Collection;

import com.bizosys.hsearch.federate.BitSetWrapper;

public class SearcherPluginOffset {
	
	int offset = 0;
	int pageSize = 10000;
	int outset = offset + pageSize;
	public boolean onSortMode = false;
	
	public SearcherPluginOffset() {
	}
	
	public void set(int offset, int pageSize) {
		System.out.println("Query > Offset: " + offset + " , Page Size:" + pageSize);
		this.offset = offset;
		this.pageSize = pageSize;
		outset = offset + pageSize;
		this.onSortMode = false;
	}

	
	public void keepPage(BitSetWrapper foundIds) {
		if ( onSortMode ) return;
		if ( null == foundIds) return;
		if ( offset < 0) return;

		int rowId = -1;
		for (int bitsIndex = foundIds.nextSetBit(0); bitsIndex >= 0; bitsIndex = foundIds.nextSetBit(bitsIndex+1)) {
			rowId++;
			if (rowId < offset ) foundIds.set(bitsIndex, false);
			if ( rowId > outset ) {
				foundIds.set(bitsIndex, foundIds.length() - 1, false);
				break;
			}
		}
	}

	public BitSetWrapper keepPage(Collection<KVRowI> sortedIds) {
		if ( null == sortedIds) return null;
		BitSetWrapper pageIds = new BitSetWrapper();
		
		int totalSize = sortedIds.size();
		int offsetScoped = ( offset > totalSize) ? totalSize : offset;
		if ( offsetScoped < 0) offsetScoped = 0;
		int outsetScoped = ( outset > totalSize) ? totalSize : outset;
		
		int index = 0; 
		for(KVRowI kv : sortedIds) {
			index++;
			if ( index < offsetScoped) continue;
			if ( index > outsetScoped) break;
			pageIds.set(kv.getId());
		}
		return pageIds;
	}

	public static void main(String[] args) {
		
		int offset = 99;
		int pageSize = 10;
		BitSetWrapper foundIds = new BitSetWrapper();
		for ( int i=0; i<200; i++) {
			if ( i % 2 == 0 ) foundIds.set(i/2);
		}
		
		int outset = offset + pageSize;
		int rowId = -1;
		for (int i = foundIds.nextSetBit(0); i >= 0; i = foundIds.nextSetBit(i+1)) {
			rowId++;
			if (rowId < offset || rowId > outset) foundIds.set(i, false);
		}

		System.out.println(foundIds.toString());
	}

}
