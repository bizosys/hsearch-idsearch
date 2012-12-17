package com.bizosys.hsearch.idsearch.table;

import java.io.IOException;

import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell4;

public interface ITable 
{
	void addSearchData (SearchTableSchema sData);
	
	byte[] toBytes(int partitionSeq) throws IOException;
	
	Cell2<Integer, Float> findIdsFromSerializedTableQuery(
			byte[] input, SearchTableQuery filter) throws IOException;
	
	Cell4<Integer, Integer, Integer, Float> findValuesFromSerializedTableQuery( 
		byte[] input, SearchTableQuery filter) throws IOException;
	
	void clear();
}
