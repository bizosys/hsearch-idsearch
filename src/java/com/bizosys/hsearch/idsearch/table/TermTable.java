package com.bizosys.hsearch.idsearch.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.Cell6;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class TermTable implements ITermTable 
{

	private Cell6<Integer, String, Integer, Integer, Integer, Float> 
		fieldHash_field_recordType_fieldType_recordId_fieldWeight = null;
	
	public TermTable()
	{
		fieldHash_field_recordType_fieldType_recordId_fieldWeight = new Cell6<Integer, String, Integer, Integer, Integer, Float>
									(SortedBytesInteger.getInstance(), SortedBytesString.getInstance(), SortedBytesInteger.getInstance(), 
											SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
		getSearchTable();
	}
	
	private void getSearchTable()
	{
		File searchFile = new File("f:\\work\\hsearch-pointcross\\reference\\searchdata");
		BufferedReader reader = null;
		InputStream stream = null;
		
		/**
		try 
		{
			stream = new FileInputStream(searchFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			
			String line;
			SearchData searchData;
			while((line=reader.readLine())!=null)
			{
				String[] lineVars = line.split("|");
				searchData = new SearchData();
				searchData.setParams(Integer.parseInt(lineVars[0]), lineVars[1], Integer.parseInt(lineVars[2]), 
						Integer.parseInt(lineVars[3]), Integer.parseInt(lineVars[4]), Float.parseFloat(lineVars[5]));
				addSearchData(searchData);
			}
		}
		catch (Exception e) {
		}
		*/
		TermTableRow searchData = new TermTableRow();
		
		System.out.println("Making Cube Start");
		for ( int i=0; i<10000000; i++) {
			searchData.setParams( "Jyoti", 1, 1, i, 0.0f);
			addSearchData(searchData);
		}
		System.out.println("Making Cube Done");

	}
	
	public void addSearchData(TermTableRow searchData)
	{
		fieldHash_field_recordType_fieldType_recordId_fieldWeight.put(searchData.fieldCode, 
				searchData.field, searchData.recordType, searchData.fieldType, searchData.recordId, searchData.fieldWeight);
	}

	@Override
	public byte[] toBytes(int partitionSeq) throws IOException 
	{
		return fieldHash_field_recordType_fieldType_recordId_fieldWeight.toBytes(new CellComparator.FloatComparator<Integer>());
	}
	
	
	@Override
	public Cell2<Integer, Float> findIdsFromSerializedTableQuery(
			byte[] input, TermQuery filter) throws IOException {
		return null;
	}
	
	@Override
	public Cell4<Integer, Integer, Integer, Float> findValuesFromSerializedTableQuery( 
		byte[] input, TermQuery filter) throws IOException {
		return null;
		
	}
	

	public void findIdsFromSerializedTableQuery(byte[] input, TermQuery filter, 
			Cell2<Integer, Float> idL, Cell4<Integer, Integer, Integer, Float> rowL) throws IOException
	{
		
		this.fieldHash_field_recordType_fieldType_recordId_fieldWeight.parseElements(input);
		
		Collection<Cell5<String, Integer, Integer, Integer, Float>> cell5L = ( filter.hasField()) ? 
				this.fieldHash_field_recordType_fieldType_recordId_fieldWeight.values(filter.getFieldCode()) : 
					this.fieldHash_field_recordType_fieldType_recordId_fieldWeight.values();
		
		if(cell5L == null) return;
		
		Collection<Cell4<Integer, Integer, Integer, Float>> cell4L = new ArrayList<Cell4<Integer,Integer,Integer,Float>>();
		Collection<Cell3<Integer, Integer, Float>> cell3L = new ArrayList<Cell3<Integer,Integer,Float>>();			
		Collection<Cell2<Integer, Float>> cell2L = new ArrayList<Cell2<Integer,Float>>();

		for (Cell5<String, Integer, Integer, Integer, Float> cell5 : cell5L) 
		{
			cell4L.clear();
			cell5.values(filter.getField(), cell4L);
			
			for (Cell4<Integer, Integer, Integer, Float> cell4 : cell4L) 
			{
				cell3L.clear();
				if(filter.hasRecordType())
					cell4.values(filter.getRecordType(), cell3L);
				else
					cell4.values(cell3L);
				
				for (Cell3<Integer, Integer, Float> cell3 : cell3L) 
				{
					cell2L.clear();
					if(filter.hasFieldType())
						cell3.values(filter.getFieldType(), cell2L);
					else
						cell3.values(cell2L);
					
					for (Cell2<Integer, Float> cell2 : cell2L) 
					{
						for (CellKeyValue<Integer, Float> kv : cell2.getMap()) {
							if ( idL != null ) idL.add(kv.getKey(), kv.getValue());
							//if ( rowL != null ) rowL.put(cell4., k2, k3, v)add(kv.getKey(), kv.getValue());
							
						}
					}
				}
				
			}
		}
	}
	
	@Override
	public void clear() 
	{
		try
		{
			fieldHash_field_recordType_fieldType_recordId_fieldWeight.getMap().clear();
		}
		catch(Exception e)
		{
		}
	}

	public static void main(String[] args)
	{
		try 
		{
			TermQuery filter = new TermQuery();
			filter.setWordFieldtypeDoctype("Jyoti", 1, 1);
			//byte[] cellBytes = stu.toBytes(0);
			//Cell2<Integer, Float> recordIds = stu.findIdsFromSerializedTableQuery(cellBytes, filter);
			//System.out.println("Final Output :" + recordIds.getMap().size());
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}
