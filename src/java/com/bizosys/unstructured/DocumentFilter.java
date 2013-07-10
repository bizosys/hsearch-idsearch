package com.bizosys.unstructured;

public abstract class DocumentFilter {
	public byte[] filter;
	
	public DocumentFilter(byte[] filter) {
		this.filter = filter;
	}

	public abstract String getTexualFilterLine();
}
