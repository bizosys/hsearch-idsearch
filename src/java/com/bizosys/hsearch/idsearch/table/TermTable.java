package com.bizosys.hsearch.idsearch.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

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
		File searchFile = new File("f:\\work\\hsearch-idsearch\\reference\\searchdata.tsv");
		BufferedReader reader = null;
		InputStream stream = null;
		
		try 
		{
			stream = new FileInputStream(searchFile); 
			reader = new BufferedReader ( new InputStreamReader(stream) );
			
			String line;
			TermTableRow searchData;
			while((line=reader.readLine())!=null)
			{
				String[] lineVars = line.split("\t");
				searchData = new TermTableRow();
				searchData.setParams(lineVars[0], Integer.parseInt(lineVars[1]), Integer.parseInt(lineVars[2]), 
						Integer.parseInt(lineVars[3]), Float.parseFloat(lineVars[4]));
				addSearchData(searchData);
			}
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}

//		TermTableRow searchData = new TermTableRow();
//		
//		System.out.println("Making Cube Start");
//		for ( int i=0; i<10000000; i++) {
//			searchData.setParams( "Jyoti", 1, 1, i, 0.0f);
//			addSearchData(searchData);
//		}
//		System.out.println("Making Cube Done");

	}
	
	public void addSearchData(TermTableRow searchData)
	{
		fieldHash_field_recordType_fieldType_recordId_fieldWeight.put(searchData.fieldCode, 
				searchData.field, searchData.recordType, searchData.fieldType, searchData.recordId, searchData.fieldWeight);
	}

	@Override
	public byte[] toBytes() throws IOException 
	{
		return fieldHash_field_recordType_fieldType_recordId_fieldWeight.toBytes(new CellComparator.FloatComparator<Integer>());
	}
	
	
	@Override
	public Cell2<Integer, Float> findIdsFromSerializedTableQuery(
			byte[] input, Map<String, TermQuery> filterMap) throws IOException 
	{
		Cell2<Integer, Float> cell2 = new Cell2<Integer, Float>(SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
		Iterator<Entry<String, TermQuery>> it = filterMap.entrySet().iterator();
		while(it.hasNext())
		{
			Entry<String, TermQuery> entry = it.next();
			TermQuery queryFilter = entry.getValue();
			findIdsFromSerializedTableQuery(input, queryFilter, cell2, null);
		}
		return cell2;
	}
	
	@Override
	public Cell4<Integer, Integer, Integer, Float> findValuesFromSerializedTableQuery( 
		byte[] input, Map<String, TermQuery> filterMap) throws IOException {
		return null;
		
	}
	

	public void findIdsFromSerializedTableQuery(byte[] input, TermQuery filter, 
			Cell2<Integer, Float> idL, Cell4<Integer, Integer, Integer, Float> rowL) throws IOException
	{
		
		this.fieldHash_field_recordType_fieldType_recordId_fieldWeight.parseElements(input);
		
		Map<Integer, Cell5<String, Integer, Integer, Integer, Float>> cell5Map = new HashMap<Integer, Cell5<String, Integer, Integer, Integer, Float>>();
		Map<Integer, Cell3<Integer, Integer, Float>> cell3Map = new HashMap<Integer, Cell3<Integer, Integer, Float>>();
		Map<Integer, Cell2<Integer, Float>> cell2Map = new HashMap<Integer, Cell2<Integer, Float>>();

		if(filter.hasField()) 
			this.fieldHash_field_recordType_fieldType_recordId_fieldWeight.getMap(filter.getFieldCode(), null, null, cell5Map);
		else
			cell5Map = this.fieldHash_field_recordType_fieldType_recordId_fieldWeight.getMap();
		 
		Iterator<Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>>> cell5MapItr = cell5Map.entrySet().iterator();  
		while (cell5MapItr.hasNext()) 
		{
			Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>> cell5E = cell5MapItr.next();
			Cell5<String, Integer, Integer, Integer, Float> cell5 = cell5E.getValue();

			Iterator<Entry<String, Cell4<Integer, Integer, Integer, Float>>> itemItr = cell5.getMap().entrySet().iterator();  
			while ( itemItr.hasNext()) 
			{
				Entry<String, Cell4<Integer, Integer, Integer, Float>> aTerm = itemItr.next();
				Cell4<Integer, Integer, Integer, Float> cell4 = aTerm.getValue();

				if(filter.hasRecordType()) 
					cell4.getMap(filter.getRecordType(), null, null, cell3Map);
				else
					cell3Map = cell4.getMap();
				
				Iterator<Entry<Integer, Cell3<Integer, Integer, Float>>> docItr = cell3Map.entrySet().iterator();
				while ( docItr.hasNext()) 
				{
					Entry<Integer, Cell3<Integer, Integer, Float>> aDoc = docItr.next();
					Integer _doc = aDoc.getKey();
					Cell3<Integer, Integer, Float> cell3 = aDoc.getValue();
							
					if(filter.hasFieldType()) 
						cell3.getMap(filter.getFieldType(), null, null, cell2Map);
					else
						cell2Map = cell3.getMap();
					
					Iterator<Entry<Integer, Cell2<Integer, Float>>> termtypeItr = cell2Map.entrySet().iterator();
					while ( termtypeItr.hasNext()) 
					{
						Entry<Integer, Cell2<Integer, Float>> word = termtypeItr.next();
						Integer _wordtype = word.getKey();
						Cell2<Integer, Float> cell2 = word.getValue();
									
						for (CellKeyValue<Integer, Float> _word : cell2.getMap()) 
						{
							if(null != rowL)
							{
								rowL.put(_doc, _wordtype, _word.getKey(), _word.getValue());
								System.out.println(_doc +", " +_wordtype +", " +_word.getKey() +", " +_word.getValue());
							}
							if(null != idL)
								idL.add(_word.getKey(), _word.getValue());
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
		TermTable tt = new TermTable();
		try 
		{
			TermQuery filter = new TermQuery();
			filter.setWordRecordtypeFieldType("searchField2", 1, 2);
			byte[] cellBytes = tt.toBytes();
			
			Cell2<Integer, Float> recordIds = 
					new Cell2<Integer, Float>(SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());
			Cell4<Integer, Integer, Integer, Float> records = 
					new Cell4<Integer, Integer, Integer, Float>(SortedBytesInteger.getInstance(), 
							SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), 
							SortedBytesFloat.getInstance());
			
			tt.findIdsFromSerializedTableQuery(cellBytes, filter, recordIds, records);
			
			for (CellKeyValue<Integer, Float> ckv : recordIds.getMap())
			{
				System.out.println("Final Output :" + ckv.getKey() +", " +ckv.getValue());
			}
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
}
