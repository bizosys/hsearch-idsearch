package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVStringInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVString;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class IndexFieldString {

	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook(values, null, isRepetable, isCompressed);
	}
	
	public static byte[] cook(Iterable<Text> values, byte[] existingData, final boolean isRepetable, final boolean isCompressed) throws IOException {
	
		IndexField fld = null;
		
		if ( isRepetable ) {
		
			fld = new IndexField() {
				HSearchTableKVStringInverted table = new HSearchTableKVStringInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, val);
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSet, String>() {

						@Override
						public void visit(BitSet k, String v) {
							table.put(k, v);
						}

					});
				}
				
			};	
			
			
		} else {
				
			fld = new IndexField() {
					
				HSearchTableKVString table = new HSearchTableKVString();
	
				@Override
				public void add(int key, String val) {
					table.put(key, val);
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<Integer, String>() {

						@Override
						public void visit(Integer k, String v) {
							table.put(k, v);
						}
					});
				}
				
			};
		}

		if ( null != existingData ) fld.append(existingData);
		return fld.index(values);
	}
}
