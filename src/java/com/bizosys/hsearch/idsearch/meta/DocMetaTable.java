package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.treetable.Cell2;

public class DocMetaTable extends Cell2<Integer, byte[]> {
	
	public DocMetaTable() {
		super(SortedBytesInteger.getInstance(), SortedBytesArray.getInstance());
	}
	
	public DocMetaTable(byte[] data) throws Exception  {
		super(SortedBytesInteger.getInstance(), SortedBytesArray.getInstance(), data);
	}

	public void add(Integer docId, DocMetaTableRow aRow) throws Exception {
		super.add(docId, aRow.toBytes());
	}

	public byte[] toBytes() throws IOException {
		return super.toBytesOnSortedData();
	}
	
	
	public DocMetaTableRow get(Integer docId) throws Exception {
		byte[] allKeysB = SortedBytesArray.getInstance().parse(data).getValueAt(0);
		if ( null == allKeysB ) return null;
		
		int size = k1Sorter.parse(allKeysB).getSize();
		for ( int i=0; i<size; i++) {
			int docIdRaw = docId.intValue();
			if ( docIdRaw ==  k1Sorter.getValueAt(i)) {
				byte[] rowB = vSorter.parse(SortedBytesArray.getInstance().parse(data).getValueAt(1)).getValueAt(i);
				return new DocMetaTableRow(docIdRaw, rowB);
			}
		}
		return null;
	}
	
	public void put(DocMetaTableRow row) throws Exception {
		byte[] rowB = row.toBytes();
		super.add(row.docId, rowB);
	}
	
}
