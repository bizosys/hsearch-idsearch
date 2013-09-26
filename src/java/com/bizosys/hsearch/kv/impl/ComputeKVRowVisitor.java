package com.bizosys.hsearch.kv.impl;

import java.util.Map;

import com.bizosys.hsearch.treetable.Cell2Visitor;

public class ComputeKVRowVisitor<V> implements Cell2Visitor<Integer, V> {
	public Map<Integer, Object> container = null;
	public int fieldSeq = 0;
	public int totalFields = 0;
	
	
	public ComputeKVRowVisitor() {
	}
	
	public ComputeKVRowVisitor(final int fieldSeq, final int totalFields, final Map<Integer, Object> container) {		
		this.fieldSeq = fieldSeq;
		this.totalFields = totalFields;
		this.container = container;
	}

	@Override
	public final void visit(final Integer k, final V v) {
		container.put(k, v);
	}
}
