package com.bizosys.hsearch.kv.dao;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public class KvRowReaderFactory {

	public interface RowReader {
		Map<Integer, Object> getAllValues(
			final String tableName, final byte[] row, 
			final ComputeKV compute, final String filterQuery,
			final HSearchProcessingInstruction inputMapperInstructions) throws IOException;
		
		Map<Integer, Object> getFilteredValues(final String tableName, 
			final byte[] row, final ComputeKV compute, final byte[] matchingIdsB,
			final BitSetWrapper matchingIds, final String filterQuery, 
			final HSearchProcessingInstruction instruction) throws IOException;
		
	}
	
	public RowReader reader = null;
	
	public KvRowReaderFactory(boolean isCache) {
		if ( isCache ) reader = new KVRowReaderCached();
		else reader = new KVRowReaderIpc();
	}
}
