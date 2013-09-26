package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.kv.dao.MapperKVBaseEmpty;
import com.bizosys.hsearch.kv.dao.ScalarFilter;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

public class BitSetRow {

	public static final Map<Integer, Object> process(final byte[] inputBytes, 
			String  filterQuery,
			final HSearchProcessingInstruction inputMapperInstructions) throws IOException{
		
		try {
			IHSearchTable table = ScalarFilter.createTable(inputMapperInstructions);
			final Map<Integer, Object> rowContainer = new HashMap<Integer, Object>();
			
			table.get(inputBytes, new HSearchQuery(filterQuery), new MapperKVBaseEmpty() {
				
				@Override
				public boolean onRowKey(int id) {
					return false;
				}
				
				@Override
				public boolean onRowCols(int k, Object value) {
					return true;
				}

				@Override
				public boolean onRowKey(BitSet ids) {
					return false;
				}

				@Override
				public boolean onRowCols(BitSet k, Object v) {
					for (int i = k.nextSetBit(0); i >= 0; i = k.nextSetBit(i+1)) {
						rowContainer.put(i, v);
					}
					return true;
				}

				@Override
				public void setMergeId(byte[] mergeId) throws IOException {
				}
			}
			);
			
			return rowContainer;
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}
}
