package com.bizosys.hsearch.idsearch.table;

import java.io.IOException;

//Now serializable 
public class SearchTableQuery 
{
	private boolean _hasField = false;
	private boolean _hasRecordType = false;
	private boolean _hasFieldType = false;

	private String field = "";
	private int fieldCode = 0;
	private int recordType = 0;
	private int fieldType = 0;
  
	public static void main(String[] args) 
	{
		SearchTableQuery i = new SearchTableQuery();
		i.setWordFieldtypeDoctype("Pramod", 1, 2);
		String ser = i.toString();
		
		try 
		{
			SearchTableQuery o;
			o = new SearchTableQuery(ser);
			System.out.println(o.toString());
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
	}
	
	public SearchTableQuery() 
	{
	}
	
	public SearchTableQuery(String input) throws IOException 
	{
		if ( input.length() < 2) throw new IOException("Invalid input - " + input);
		if ( input.charAt(0) == 'T') _hasField = true;
		if ( input.charAt(1) == 'T') _hasFieldType = true;
		if ( input.charAt(2) == 'T') _hasRecordType = true;
		  
		String text = input.substring(2);
		  
		int position = -1;
		int index1 = 0;
		int index2 = text.indexOf('|');
		String token = null;
		while (index2 >= 0) 
		{
			token = text.substring(index1, index2);
			populate(position++, token);
			index1 = index2 + 1;
			index2 = text.indexOf('|', index1);
		}
		            
		if (index1 < text.length() - 1)
			populate(position++, text.substring(index1));
	}
	
	private void populate (int position, String token) throws IOException 
	{
		if (token.length() == 0 ) return;
		switch (position) 
		{
			case 0:
				this.field = token;
				this.fieldCode = token.hashCode();
				break;
			case 1:
				this.fieldType = Integer.parseInt(token);
				break;
			case 2:
				this.recordType = Integer.parseInt(token);
				break;
		}
	}
	
	@Override
	public String toString() 
	{
		StringBuilder sb = new StringBuilder();
			  
		if ( hasField()) sb.append('T');
		else sb.append('F');
		  
		if ( hasFieldType()) sb.append('T');
		else sb.append('F');
		
		if ( hasRecordType()) sb.append('T');
		else sb.append('F');
		
		sb.append('|').append(field);
		sb.append('|').append(fieldType);
		sb.append('|').append(recordType);
			  
		return sb.toString();
	}
	
	public void setWordFieldtypeDoctype(String field, int recordType, int fieldType)
	{
		setField(field);
		setRecordType(recordType);
		setFieldType(fieldType);
	}

	public String getField() {
		return field;
	}

	public int getFieldCode() {
		return fieldCode;
	}

	public void setField(String field) 
	{
		this.field = field;
		this.fieldCode = field.hashCode();
		this._hasField = true;
	}

	public int getRecordType() {
		return recordType;
	}

	public void setRecordType(int recordType) 
	{
		this.recordType = recordType;
		this._hasRecordType = true;
	}

	public int getFieldType() {
		return fieldType;
	}

	public void setFieldType(int fieldType) 
	{
		this.fieldType = fieldType;
		this._hasFieldType = true;
	}

	public boolean hasField() {
		return _hasField;
	}

	public boolean hasRecordType() {
		return _hasRecordType;
	}

	public boolean hasFieldType() {
		return _hasFieldType;
	}

}
