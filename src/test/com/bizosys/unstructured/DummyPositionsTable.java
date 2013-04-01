package com.bizosys.unstructured;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.unstructured.IIndexPositionsTable;

public class DummyPositionsTable implements IIndexPositionsTable {

	@Override
	public void get(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException, NumberFormatException {
	}

	@Override
	public void keySet(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException {
	}

	@Override
	public void keyValues(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException {
	}

	@Override
	public void values(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException {
	}

	@Override
	public void put(Integer docId, Integer docType, Integer fieldType, Integer hashCode, byte[] positions) {
		try {
			System.out.println(docId + "\t" + docType + "\t" + fieldType + "\t" + hashCode + "\t" +  
					SortedBytesInteger.getInstance().parse(positions).values().toString());
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}

	@Override
	public byte[] toBytes() {
		return null;
	}

	@Override
	public void clear() {
	}


}
