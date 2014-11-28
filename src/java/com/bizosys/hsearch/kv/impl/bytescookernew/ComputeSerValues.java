package com.bizosys.hsearch.kv.impl.bytescookernew;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.util.HSearchLog;

public abstract class ComputeSerValues<T> {

	public void compute (final Cell2<Integer, T> cell2, final byte[] input) throws IOException {

		SortedBytesArray sba = SortedBytesArray.getInstanceArr();
		sba.parse(input, 0, input.length);

		Reference allValsA = sba.getValueAtReference(0);
		Reference allValsB = sba.getValueAtReference(1);

		ISortedByte<Integer> k1Sorter = cell2.k1Sorter;
		k1Sorter.parse(input,allValsA.offset, allValsA.length);

		ISortedByte<T> vSorter = cell2.vSorter;
		vSorter.parse(input,allValsB.offset, allValsB.length);

		int sizeK = k1Sorter.getSize();
		int sizeV = vSorter.getSize();
		if ( sizeK != sizeV ) onKVSizeMismatch(sizeK, sizeV, input);
		
		for ( int i=0; i<sizeK; i++) {
			visit(k1Sorter.getValueAt(i), vSorter.getValueAt(i));
		}
	}

	public final void onKVSizeMismatch(final int sizeK, final int sizeV, final byte[] data) throws IOException {

		if ( sizeK != sizeV ) {
			int truncate = (null == data) ? 0 : data.length;
			if ( truncate > 101 )  truncate = 100;
			
			String truncatedData = (truncate > 0 ) ? new String(data, 0 , truncate) : "EMPTY";
			if ( null != data) HSearchLog.l.fatal("Unable to deserialize data." + truncatedData );
			throw new IOException("Unable to deserialize data. Mismatch : " + sizeK + " != " + sizeV + "\n" + truncatedData);
		}
		
		if ( sizeK != sizeV ) {
			if ( null != data) HSearchLog.l.fatal(
				"Unable to deserialize data because Mismatch of key and value : " + sizeK + " != " + sizeV);
			throw new IOException("Unable to deserialize data. Mismatch : " + sizeK + " != " + sizeV);
		}
	}	

	public abstract void visit(final int rowid, final T val);
}
