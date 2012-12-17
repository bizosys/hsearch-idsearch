package com.bizosys.hsearch.idsearch.table;

public class SearchTableSchema
{
	public int fieldCode;
	public String field;
	public int recordType;
	public int fieldType;
	public int recordId;
	public float fieldWeight;
	
	public void setParams(String _field, Integer _recordType, Integer _fieldType, 
			Integer _recordId, Float _fieldWeight)
	{
		fieldCode = _field.hashCode();
		field = _field;
		recordType = _recordType;
		fieldType = _fieldType;
		recordId = _recordId;
		fieldWeight = _fieldWeight;
	}
	
	public String toString()
	{
		return fieldCode +"-" +field +"-" +recordType +"-" +fieldType +"-" +recordId +"-" +fieldWeight;
	}
}
