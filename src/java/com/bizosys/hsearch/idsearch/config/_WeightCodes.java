package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesChar;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class _WeightCodes {
	
	Map<String, Byte> nameIds = new HashMap<String, Byte>();
	public _WeightCodes (byte[] data) throws IOException  {
		
		Cell2<String, Byte> codes = new Cell2<String, Byte>(
			SortedBytesString.getInstance(), SortedBytesChar.getInstance(), data);
		
		for (CellKeyValue<String, Byte> ckv: codes.getMap()) {
			nameIds.put(ckv.key, ckv.value);
		}
	}
	
	
	public Byte getCode(String name) throws IOException {
		if ( ! this.nameIds.containsKey(name)) {
			throw new IOException(
				"Name is not available. Current data size :" + this.nameIds.size());
		}
		return this.nameIds.get(name);
	}
	
	public boolean hasCode(String name) throws IOException {
		return this.nameIds.containsKey(name);
	}
	
	public static _WeightCodes.Builder builder() {
		return new _WeightCodes.Builder();
	}	
	
	public static class Builder {
		
		Cell2<String, Byte> nameIds = null;
		public Builder() {
			nameIds = new Cell2<String, Byte>(
				SortedBytesString.getInstance(), SortedBytesChar.getInstance());
		}		
		
		public byte[] toBytes() throws IOException {
			nameIds.sort(new CellComparator.ByteComparator<String>());
			return nameIds.toBytesOnSortedData();
		}		
		
		public Builder add(Map<String, Byte> codes) {
			for (String name : codes.keySet()) {
				nameIds.add(name, codes.get(name));
			}
			return this;
		}
		
		public Builder add(String name, Byte id) {
			nameIds.add(name, id);
			return this;
		}

	}

}
