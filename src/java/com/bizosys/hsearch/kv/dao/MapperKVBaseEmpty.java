package com.bizosys.hsearch.kv.dao;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.kv.dao.MapperKVBase.TablePartsCallback;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public abstract class MapperKVBaseEmpty extends MapperKVBase implements TablePartsCallback{
	
	@Override
	public void setOutputType(HSearchProcessingInstruction outputTypeCode){}
	
	@Override
	public void onReadComplete() {}
	
	@Override
	public BitSetOrSet getUniqueMatchingDocumentIds() throws IOException {return null;}
	
	@Override
	public void getResultSingleQuery(Collection<byte[]> rows) throws IOException {}
	
	@Override
	public void getResultMultiQuery(BitSetOrSet matchedIds,Collection<byte[]> rows) throws IOException {}
	
	@Override
	public void clear() {}
	
	@Override
	public TablePartsCallback getPart() {
		return this;
	}
	
	@Override
	public abstract boolean onRowCols(int key, Object value);

	@Override
	public abstract boolean onRowKey(int id);

}
