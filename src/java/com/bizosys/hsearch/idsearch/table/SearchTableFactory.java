package com.bizosys.hsearch.idsearch.table;

public class SearchTableFactory 
{
	public static ITable getTable() 
	{
		return new SearchTableUtil();
		
	}
}
