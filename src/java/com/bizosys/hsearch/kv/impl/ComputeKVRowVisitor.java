package com.bizosys.hsearch.kv.impl;

import java.util.Map;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class ComputeKVRowVisitor<V> implements Cell2Visitor<Integer, V> {
	public Map<Integer, Object> container = null;
	private BitSetOrSet matchIds = null;
	private boolean isMatchedIds = false;
	public int fieldSeq = 0;
	public int totalFields = 0;
	
	
	public ComputeKVRowVisitor() {
	}
	
	public ComputeKVRowVisitor(final BitSetOrSet matchIds) {
		this.matchIds = matchIds;
		isMatchedIds = ( null != this.matchIds);
	}

	public ComputeKVRowVisitor(final BitSetOrSet matchIds, int fieldSeq, int totalFields, Map<Integer, Object> container) {
		this.matchIds = matchIds;
		isMatchedIds = ( null != this.matchIds);
		
		this.fieldSeq = fieldSeq;
		this.totalFields = totalFields;
		this.container = container;
	}

	@Override
	public final void visit(final Integer k, final V v) {
		if ( isMatchedIds && null != k) {
			if ( matchIds.contains(k) ) {
				container.put(k, v);
			}
		} else {
			container.put(k, v);
		}
	}
}
