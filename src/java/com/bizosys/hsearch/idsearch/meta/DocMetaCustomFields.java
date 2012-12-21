package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class DocMetaCustomFields {
	
	Map<String, String> values = null;
	
	public DocMetaCustomFields(Map<String, String> values) {
		this.values = values;
	}
	
	public byte[] toBytes() throws IOException{
		Cell2<String, String> others = new Cell2<String, String>(
			SortedBytesString.getInstance(), SortedBytesString.getInstance());
		for (String key: values.keySet()) {
			others.add(key, values.get(key));
		}
		others.sort(new CellComparator.TextComparator<String>());
		return others.toBytesOnSortedData();
	}
	
	public static DocMetaCustomFields build(byte[] data) throws IOException {
		Map<String, String> values = new HashMap<String, String>();
		return build(data, values);
	}
	
	public static DocMetaCustomFields build(byte[] data, Map<String, String> values) throws IOException {
		Cell2<String, String> cell = new Cell2<String, String>
				(SortedBytesString.getInstance(), SortedBytesString.getInstance(), data);
		
		cell.populate(values);
		return new DocMetaCustomFields(values);
	}

	public String toString() {
		return this.values.toString();
	}
	
	public static void main(String[] args) throws IOException {
		Map<String, String> others = new HashMap<String, String>();
		others.put("age" , "23");
		others.put("sex" , "male");
		others.put("location" , "bangalore");
		
		
		DocMetaCustomFields serWeight = new DocMetaCustomFields(others);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		Map<String, String> foundValues = new HashMap<String, String>();
		for ( int i=0; i<1000000; i++) {
			foundValues.clear();
			DocMetaCustomFields.build(ser, foundValues);
		}
		long end = System.currentTimeMillis();
		System.out.println (DocMetaCustomFields.build(ser).values.keySet().toString() + "   in " + (end - start) );
	}
}
