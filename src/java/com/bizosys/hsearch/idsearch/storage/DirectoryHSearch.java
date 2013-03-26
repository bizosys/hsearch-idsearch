package com.bizosys.hsearch.idsearch.storage;

import java.io.IOException;

import com.bizosys.hsearch.idsearch.storage.donotmodify.HSearchTableDocuments;

public class DirectoryHSearch {
	HSearchTableDocuments table = new HSearchTableDocuments();
	
	public void put(int docType, int fieldType, int hashCode, String token, int docId, float boost) {
		table.put(docType, fieldType, hashCode, token, docId, boost);		
	}
	
	public void clear() throws IOException {
		this.table.clear();
	}
	
	public byte[] toBytes() throws IOException {
		return table.toBytes();
	}
}
