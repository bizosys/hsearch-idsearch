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

import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.treetable.Cell2Visitor;

/**
 * A visitor class for a given row.
 * @author shubhendu
 *
 * @param <V>
 */
public final class ComputeKVRowVisitor<V> implements Cell2Visitor<Integer, V> {
	public Map<Integer, Object> container = null;
	private BitSetWrapper matchingIds = null;
	private boolean isMatchingIds = false; 
	
	public ComputeKVRowVisitor() {
	}
	
	/**
	 * Sets the matching ids.
	 * @param matchingIds
	 */
	public void setMatchingIds(BitSetWrapper matchingIds) {
		this.matchingIds = matchingIds;
		this.isMatchingIds = ( null != this.matchingIds );
	}
	
	/**
	 * Initalizes the container
	 * @param container
	 */
	public ComputeKVRowVisitor(final Map<Integer, Object> container) {		
		this.container = container;
	}
	
	/**
	 * For each key-value pair this method is visited.
	 */
	@Override
	public final void visit(final Integer k, final V v) {
		if ( this.isMatchingIds ) {
			if ( ! this.matchingIds.get(k) ) return; 
		}
		container.put(k, v);
	}
}
