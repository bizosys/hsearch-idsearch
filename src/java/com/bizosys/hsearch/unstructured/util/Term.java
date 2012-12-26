package com.bizosys.hsearch.unstructured.util;

import com.bizosys.hsearch.idsearch.table.TermTableRow;

public class Term {
	
	private static final ThreadLocal<TermTableRow> concurrentRow = new ThreadLocal<TermTableRow>() {
		@Override protected TermTableRow initialValue() {
            return new TermTableRow();
		}
	}; 
			
	public Term(int docId, String token, int docType, int fieldName, int weight, int offset) {
		concurrentRow.get().setParams(token, docType, fieldName, docId, offset);
	}
	
	public TermTableRow toTermTableRow() {
		return concurrentRow.get();
	}
}
