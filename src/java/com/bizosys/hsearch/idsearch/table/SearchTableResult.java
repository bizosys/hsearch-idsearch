package com.bizosys.hsearch.idsearch.table;

import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell4;

//Now serializable
public class SearchTableResult 
{
	public Cell2<Integer, Float> recordIds;
	public Cell4<Integer, Integer, Integer, Float> recordType_fieldType_recordId_fieldWeight; 
	
	public SearchTableResult()
	{
	}
	
}
