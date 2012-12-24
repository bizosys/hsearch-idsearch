package com.bizosys.hsearch.idsearch.table;

public class TermTableRow
{
	public int fieldCode;
	public String field;
	public int recordType;
	public int fieldType;
	public int recordId;
	public float fieldWeight;
	
	public TermTableRow() {
	}
	
	public TermTableRow(String field, int recordType, int fieldType, int recordId, float fieldWeight) {
		this.field = field;
		this.fieldCode = this.field.hashCode();
		this.recordType = recordType;
		this.fieldType = fieldType;
		this.recordId = recordId;
		this.fieldWeight = fieldWeight;
	}
	
	public void setParams(String _field, int _recordType, int _fieldType, int _recordId, float _fieldWeight)
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
