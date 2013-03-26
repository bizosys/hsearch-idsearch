package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.treetable.Cell2;

public class DocumentMetaTable extends Cell2<Integer, byte[]> {
	
	public DocumentMetaTable() {
		super(SortedBytesInteger.getInstance(), SortedBytesArray.getInstance());
	}
	
	public DocumentMetaTable(byte[] data) throws Exception  {
		super(SortedBytesInteger.getInstance(), SortedBytesArray.getInstance(), data);
	}

	public void add(Integer docId, DocumentMeta aRow) throws Exception {
		super.add(docId, aRow.toBytes());
	}

	public byte[] toBytes() throws IOException {
		return super.toBytesOnSortedData();
	}
	
	
	public DocumentMeta get(Integer docId) throws Exception {
		SortedBytesArray sba = SortedBytesArray.getInstanceArr();
		sba.parse(data.data, data.offset, data.length);
		
		Reference keysRef = sba.getValueAtReference(0);
		if ( null == keysRef ) return null;
		
		Reference valRef = sba.getValueAtReference(1);
		if ( null == valRef ) return null;
		
		int size = k1Sorter.parse(data.data, keysRef.offset, keysRef.length).getSize();
		for ( int i=0; i<size; i++) {
			int docIdRaw = docId.intValue();

			if ( docIdRaw ==  k1Sorter.getValueAt(i)) {

				byte[] rowB = vSorter.parse(this.data.data, valRef.offset, valRef.length).getValueAt(i);
				return new DocumentMeta(rowB);
			}
		}
		return null;
	}
	
	public void put(Integer rowId, DocumentMeta row) throws Exception {
		byte[] rowB = row.toBytes();
		super.add(rowId, rowB);
	}
	
}
