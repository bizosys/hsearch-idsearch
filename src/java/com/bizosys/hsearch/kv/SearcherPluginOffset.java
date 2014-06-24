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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;

/**
 * 
 * This plugin class is used for calculation of pages that need to be returned.
 *
 */
public class SearcherPluginOffset {
	
	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	int offset = 0;
	int pageSize = 10000;
	int outset = offset + pageSize;
	public boolean disabled = false;
	
	public SearcherPluginOffset() {
	}
	
	public void set(int offset, int pageSize) {
		if ( DEBUG_ENABLED) IdSearchLog.l.debug(
			"Query > Offset: " + offset + " , Page Size:" + pageSize);
		this.offset = offset;
		this.pageSize = pageSize;
		outset = offset + pageSize;
		this.disabled = false;
	}

	
	public void keepPage(BitSetWrapper foundIds) {

		if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Pagination with NO Default Ranking.");
		
		if ( disabled ) return;
		if ( null == foundIds) return;
		if ( offset < 0) return;

		int rowId = -1;
		for (int bitsIndex = foundIds.nextSetBit(0); bitsIndex >= 0; bitsIndex = foundIds.nextSetBit(bitsIndex+1)) {
			rowId++;
			if (rowId < offset ) foundIds.set(bitsIndex, false);
			if ( rowId >= outset ) {
				foundIds.set(bitsIndex, foundIds.length() - 1, false);
				break;
			}
		}
	}
	
	/**
	 * This works on multiphrases. Keeps only those Ids which matches maximum phrases.
	 * 5 records matches all 3 words and other 95 records are only 2 words.
	 * For 1 - 100 Pagination, this will not float the top 3 words, It will mix all.
	 * @param foundIds
	 * @param uniqueRankBuckets
	 */
	public final void keepPage(final BitSetWrapper foundIds, final Map<Integer, BitSetWrapper> uniqueRankBuckets) {
		
		if ( disabled ) return;
		if ( null == foundIds) return;
		if ( offset < 0) return;
		
		int uniqueRankBucketsT = ( null == uniqueRankBuckets) ? 0 : uniqueRankBuckets.size();  
		if ( 0 == uniqueRankBucketsT) {
			this.keepPage(foundIds);
			return;
		}
		
		/**
		 * Picks the documents which belong to the maximum matching phrases.
		 */
		int rankBucketsT = uniqueRankBuckets.size();
		
		foundIds.clear();
		
		int rowSeq = 0;

		for ( int bucketNo=rankBucketsT; bucketNo>0 ;bucketNo-- ) {

			if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Looking Bucket :" + bucketNo + "\t Row Seq = " + rowSeq + "\t, Found Ids = " + foundIds.cardinality());
			
			BitSetWrapper aBucket = uniqueRankBuckets.get(bucketNo);
			if ( null == aBucket) continue;
			rowSeq += aBucket.cardinality();
			
			/**
			 * The offset has not reached yet.
			 */
			if ( rowSeq < offset ) continue; 

			if ( offset == 0 && rowSeq == outset ) { //Whole is applicable

				foundIds.or(aBucket);
				if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Complete Processing Bucket :" + bucketNo + "\t Row Seq = " + rowSeq + "\t, Found Ids = " + foundIds.cardinality());
			
			} else {
			
				rowSeq -= aBucket.cardinality();
				for (int bitsIndex = aBucket.nextSetBit(0); bitsIndex >= 0; bitsIndex = aBucket.nextSetBit(bitsIndex+1)) {
					rowSeq++;
					if (rowSeq < offset ) continue;
					foundIds.set(bitsIndex);
					if (rowSeq >= outset ) break;
				}
				if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Partial Processing Bucket :" + bucketNo + "\t Row Seq = " + rowSeq + "\t, Found Ids = " + foundIds.cardinality());
				
			}
			if (rowSeq >= outset ) break;
		}
		if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Final Found Ids :" + foundIds.cardinality());
	}
	
	public final Set<KVRowI> defaultRankSortPage(Set<KVRowI> sortedIds, final Map<Integer, BitSetWrapper> uniqueRankBuckets) {
		
		if ( null == sortedIds) return null;
		if ( null == uniqueRankBuckets) return sortedIds;
		
		/**
		 * Picks the documents which belong to the maximum matching phrases.
		 */
		int rankBucketsT = uniqueRankBuckets.size();
		List<WeightedKVRow> result = new ArrayList<WeightedKVRow>(sortedIds.size());
		
		boolean isAdded = false;
		for (KVRowI row : sortedIds) {
			
			int id = row.getId();
			isAdded = false;
			for ( int bucketNo=rankBucketsT; bucketNo>0 ;bucketNo-- ) {

				BitSetWrapper aBucket = uniqueRankBuckets.get(bucketNo);
				if ( null == aBucket) continue;
				if ( aBucket.get(id) ) {
					result.add(new WeightedKVRow(row,bucketNo));
					isAdded = true;
					break;
				}
			}
			if ( !isAdded) result.add(new WeightedKVRow(row,0));
 		}
		
		Collections.sort(result);
		Set<KVRowI> resultSet = new LinkedHashSet<KVRowI>(result.size());
		for (WeightedKVRow row : result) {
			resultSet.add(row.kvRow);
		}
		
		if ( DEBUG_ENABLED ) IdSearchLog.l.debug("Ordered Ids:" + resultSet.size());
		return resultSet;
	}	

	public BitSetWrapper keepPage(Collection<KVRowI> sortedIds) {
		if ( null == sortedIds) return null;
		if ( disabled ) return null;
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

		if ( DEBUG_ENABLED) IdSearchLog.l.debug ( foundIds.toString());
	}

}
