package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.kv.dao.KVRowReader;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;
/**
public class MultiStorageReader {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	public KVDocIndexer indexer = new KVDocIndexer();
	
	public String tableName;
	public List<byte[]> rowIds;
	public String filterQuery;
	public HSearchProcessingInstruction instruction = null;
	
	byte[] matchingIdsB = null;
	
	public MultiStorageReader(final String tableName, final List<byte[]> rowIds, final byte[] matchingIdsB, final String filterQuery, 
		final HSearchProcessingInstruction instruction) {

		this.tableName = tableName;
		this.rowIds = rowIds;
		this.filterQuery = filterQuery;
		this.instruction = instruction;
		this.matchingIdsB = matchingIdsB;
	}

	public final void readStorageValues(Map<String, Object> finalResult) throws IOException {

		try {
			long start = System.currentTimeMillis();

			List<CellKeyValue<byte[], byte[]>> res = KVRowReader.getBatchFilteredValues(
				this.tableName, this.rowIds, this.matchingIdsB, this.filterQuery, instruction);
			
			ComputeKV compute = new ComputeKV();
			compute.kvType = (instruction.getOutputType() == Datatype.FREQUENCY_INDEX) ? Datatype.STRING : instruction.getOutputType();
			compute.kvRepeatation = instruction.getProcessingHint().startsWith("true");
			compute.isCompressed = instruction.getProcessingHint().endsWith("true");

			for (CellKeyValue<byte[], byte[]> cellKeyValue : res) {
				compute.rowContainer = new HashMap<Integer, Object>();
				compute.put(cellKeyValue.value);
				String k = new String(cellKeyValue.key);
				int fldIndex = k.indexOf('_');
				System.out.println(k + "]]]\t" + k.substring(fldIndex+1));
				finalResult.put(k.substring(fldIndex+1), compute.rowContainer);
			}
			
			if(DEBUG_ENABLED){
				long end = System.currentTimeMillis();
				HSearchLog.l.debug(" Fetch time " + (end - start) );
			}

		} catch (Exception e) {
			String msg = e.getMessage() + "\nMultiGet For rowids = "+ rowIds.size() + " query = " + filterQuery;
			HSearchLog.l.fatal("ReadStorage Exception " + msg , e );
			e.printStackTrace();
			throw new IOException(msg, e);
		}

		
	}

}
*/

