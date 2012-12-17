package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.Storable;

public class DocMetaFilters {
	
	public byte state = 0;
	public int documentType = -1;
	public long createdOn = -1;
	public long modifiedOn = -1;

	public DocMetaFilters(byte state, int documentType, long createdOn, long modifiedOn) {
		this.state = state;
		this.documentType = documentType;
		this.createdOn = createdOn;
		this.modifiedOn = modifiedOn;
	}
	
	public byte[] toBytes() throws IOException{
		
		byte[] docMetaFilterB = new byte[25];
		docMetaFilterB[0] = state;
		
		System.arraycopy(Storable.putInt(documentType), 0, docMetaFilterB, 1, 4);
		System.arraycopy(Storable.putLong(createdOn), 0, docMetaFilterB, 5, 8);
		System.arraycopy(Storable.putLong(modifiedOn), 0, docMetaFilterB, 13, 8);
		return docMetaFilterB;
	}
	
	
	public static DocMetaFilters build(byte[] data) throws IOException {
		return new DocMetaFilters( 
			data[0],
			Storable.getInt(1, data),
			Storable.getLong(5, data),
			Storable.getLong(13, data)
		);
	}
	
	public String toString() {
		return this.state + ":" + this.documentType + ":" + this.createdOn + ":" + this.modifiedOn;
	}
	
	public static void main(String[] args) throws IOException {
		DocMetaFilters serWeight = new DocMetaFilters((byte)1,23,55555555555555L,9666666666669L);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		DocMetaFilters dm = null;
		for ( int i=0; i<1000000; i++) {
			dm = DocMetaFilters.build(ser);
		}
		long end = System.currentTimeMillis();
		System.out.println ( dm.toString() + "   in " + (end - start) );
	}
}
