package com.bizosys.hsearch.kv.impl;

import java.util.BitSet;
import java.util.Map;

import com.bizosys.hsearch.treetable.Cell2Visitor;

public final class ComputeKVRowVisitor<V> implements Cell2Visitor<Integer, V> {
	public Map<Integer, Object> container = null;
	private BitSet matchingIds = null;
	private boolean isMatchingIds = false; 
	
	public ComputeKVRowVisitor() {
	}
	
	public void setMatchingIds(BitSet matchingIds) {
		this.matchingIds = matchingIds;
		this.isMatchingIds = ( null != this.matchingIds );
	}
	
	public ComputeKVRowVisitor(final Map<Integer, Object> container) {		
		this.container = container;
	}
	
	@Override
	public final void visit(final Integer k, final V v) {
		if ( this.isMatchingIds ) {
			if ( ! this.matchingIds.get(k) ) return; 
		}
		container.put(k, v);
	}
}
