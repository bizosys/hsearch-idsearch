package com.bizosys.hsearch.kv.impl;

import java.util.Map;

import com.bizosys.hsearch.treetable.Cell2Visitor;

public final class ComputeKVRowVisitor<V> implements Cell2Visitor<Integer, V> {
	public Map<Integer, Object> container = null;
	
	
	public ComputeKVRowVisitor() {
	}
	
	public ComputeKVRowVisitor(final Map<Integer, Object> container) {		
		this.container = container;
	}
	
	@Override
	public final void visit(final Integer k, final V v) {
		container.put(k, v);
	}
}
