package com.bizosys.hsearch.idsearch.table;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell4;

public interface ITermTable 
{
	void addSearchData(TermTableRow row);
	
	byte[] toBytes() throws IOException;
	
//	Cell2<Integer, Float> findIdsFromSerializedTableQuery(
//			byte[] input, TermQuery filter) throws IOException;
	
//	Cell4<Integer, Integer, Integer, Float> findValuesFromSerializedTableQuery( 
//			byte[] input, TermQuery filter) throws IOException;

	Cell2<Integer, Float> findIdsFromSerializedTableQuery(
			byte[] input, Map<String, TermQuery> filterMap) throws IOException;

	Cell4<Integer, Integer, Integer, Float> findValuesFromSerializedTableQuery( 
		byte[] input, Map<String, TermQuery> filterMap) throws IOException;
	
	void clear();
}
