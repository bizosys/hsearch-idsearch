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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;

public final class PhraseWeightComputation {

	
	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	public static final void dedupBuckets(final Map<Integer, BitSetWrapper> rankBuckets) {
		
		BitSetWrapper dups = new BitSetWrapper();
		int rankBucketsT = rankBuckets.size();
		for ( int bucketNo = rankBucketsT; bucketNo > 0 ; bucketNo-- ) {
			BitSetWrapper aBucket =  rankBuckets.get(bucketNo);
			if ( bucketNo != rankBucketsT)  aBucket.andNot(dups); //Leave the first one
			dups.or(aBucket);
		}
		
		if ( DEBUG_ENABLED) {
			for ( int bucketNo = rankBucketsT; bucketNo > 0 ; bucketNo-- ) {
				IdSearchLog.l.debug(bucketNo + "\t" + rankBuckets.get(bucketNo).cardinality());
			}	
		}
	}
	
	/**
	 * Reset the match Ids based on the internal Fetch Limit
	 * @param internalFetchLimit
	 * @param rankBuckets
	 * @param matchIds
	 */
	public static final void internalFetchLimitTrimming(final int internalFetchLimit,
			final Map<Integer, BitSetWrapper> uniqueRankBuckets, final BitSetOrSet matchIds) {
		
		//Return if no internal fetch limit is set
		if ( internalFetchLimit < 0 ) return;
		if ( null == matchIds) return;

		//Return if No match Ids
		BitSetWrapper matchIdBits = matchIds.getDocumentSequences();
		if ( null == matchIdBits) return;
		int matchIdBitsT = matchIdBits.cardinality();
		if ( 0 == matchIdBitsT) return;

		//Found less Ids than the fetch limit. Nothing to do
		if ( matchIdBitsT < internalFetchLimit) return;  

		/**
		 * Calculate phrase weight
		 */

		int totalBuckets = ( null == uniqueRankBuckets ) ? 0 : uniqueRankBuckets.size();
		
		if ( totalBuckets > 0 ) {
			
			BitSetWrapper trimmedSet = new BitSetWrapper();
			
			for ( int i=totalBuckets; i>0; i--) {
				BitSetWrapper aPhrase = uniqueRankBuckets.get(i);
				if ( null == aPhrase) continue;
				trimmedSet.or(aPhrase);
				if ( trimmedSet.cardinality() > internalFetchLimit) break; //Just pick better rows
			}

			int trimmedSetT = trimmedSet.cardinality();
			
			if ( trimmedSetT < matchIdBitsT) {
				matchIdBits.and(trimmedSet);
			}
			matchIdBitsT = matchIdBits.cardinality();
			
			if ( DEBUG_ENABLED ) IdSearchLog.l.debug( "MatchedId Bits(Count) :" + 
				matchIdBitsT + "\t TrimmedId Bits(Count) :" + 
				trimmedSetT + "\t Limit :" + internalFetchLimit);
		}
			
		/**
		 * Trim the size to fit the bucket from reverse side.
		 * Assumption, In the append mode, new documents are added at the end.
		 */

		//Set the index to the last bit
		int matchIdBitsI = matchIdBits.size();
		if ( ! matchIdBits.get(matchIdBitsI) ) matchIdBitsI = matchIdBits.previousSetBit(matchIdBitsI);
		
		if ( internalFetchLimit < matchIdBitsT) {
			int count = 1; // Already set to the last bit
			while ( count < internalFetchLimit  ) {
				matchIdBitsI = matchIdBits.previousSetBit(matchIdBitsI-1);
				if ( matchIdBitsI <= 0 ) break;
				count++;
			}
		}
		if ( matchIdBitsI > 0 ) matchIdBits.clear(0, matchIdBitsI-1);

		if ( DEBUG_ENABLED ) IdSearchLog.l.debug( 
			"Final Trimmed Set, MatchedId Bits(Count) :" +  matchIdBits.cardinality() );
	}

	/**
	 * Calculate phrase weight buckets. Means which all doc bitsets matches n phrases.
	 * It will have total n-1 buckets. 
	 * @param phrases
	 * @return
	 */
	public final static Map<Integer, BitSetWrapper> calculatePhraseWeight(final List<BitSetWrapper> phrases) {
		
		if ( null == phrases) return null;
		int phrasesT = phrases.size();
		Map<Integer, BitSetWrapper> phraseWeightBuckets = new HashMap<Integer, BitSetWrapper>();
		
		for (int subSequence=phrasesT; subSequence> 0; subSequence--) {
			
			for ( int wordPosition=0; wordPosition<= phrasesT - subSequence; wordPosition++ ) {
				
				BitSetWrapper wrapper = new BitSetWrapper();
				for (int pos=0; pos < subSequence ; pos++) {
					if ( pos > 0) {
						wrapper.and(phrases.get(wordPosition+pos));
					} else {
						wrapper.or(phrases.get(wordPosition+pos));
					}
				}
				
				if (phraseWeightBuckets.containsKey(subSequence)) {
					phraseWeightBuckets.get(subSequence).or(wrapper);
				} else {
					phraseWeightBuckets.put(subSequence, wrapper);
				}
			}
		}
		
		if ( DEBUG_ENABLED ) {
			IdSearchLog.l.debug("Rank Buckets : "  + phraseWeightBuckets.size());
			for (Integer phraseCount : phraseWeightBuckets.keySet()) {
				IdSearchLog.l.debug("Total Phrases : "  + phraseCount + 
					"\t, Found " + phraseWeightBuckets.get(phraseCount).cardinality() + "\t" + phraseWeightBuckets.get(phraseCount).nextSetBit(0));
			}
		}
		return phraseWeightBuckets;
	}
	
	/**
		big = 1,2,3,4,5,7,8
		dog = 5,6,7,8
		eating = 7,8
	 */
	
	public static void main(String[] args) {
		
		BitSetWrapper[] phrases = new BitSetWrapper[3];
		phrases[0] = new BitSetWrapper();
		for ( int i=1; i<=5; i++ ) phrases[0].set(i); 
		phrases[0].set(7);phrases[0].set(8);
		
		phrases[1] = new BitSetWrapper();
		for ( int i=5; i<=8; i++ ) phrases[1].set(i);
		
		phrases[2] = new BitSetWrapper();
		phrases[2].set(7); phrases[2].set(8);
		
		List<BitSetWrapper> l = new ArrayList<BitSetWrapper>();
		for (BitSetWrapper p : phrases) l.add(p);
		
		Map<Integer, BitSetWrapper> phraseMatchedBuckets = calculatePhraseWeight(l);
		
		for (int seq : phraseMatchedBuckets.keySet()) {
			System.out.println("Final > " + seq + "\t" + phraseMatchedBuckets.get(seq).toString());
		}
		dedupBuckets(phraseMatchedBuckets);
		
		for (int seq : phraseMatchedBuckets.keySet()) {
			System.out.println("Final > " + seq + "\t" + phraseMatchedBuckets.get(seq).toString());
		}

		BitSetOrSet finalIds = new BitSetOrSet();
		BitSetWrapper bits = new BitSetWrapper();
		
		for ( int i=1; i<=5; i++ ) bits.set(i); 
		bits.set(7);bits.set(8);
		
		finalIds.setDocumentSequences(bits);
		
		internalFetchLimitTrimming(4, phraseMatchedBuckets, finalIds);
		System.out.println("\n\nfinal result " + finalIds.getDocumentSequences().toString());
	}
	
}
