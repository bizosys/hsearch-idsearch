package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVIntegerInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class IndexFieldInteger {

	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook(values, null, isRepetable, isCompressed);
	}
	
	public static byte[] cook(Iterable<Text> values, final byte[] exstinData, 
			final boolean isRepetable, final boolean isCompressed ) throws IOException {
		
		IndexField fld = null;
		
		if ( isRepetable ) {
		
			fld = new IndexField() {
				HSearchTableKVIntegerInverted table = new HSearchTableKVIntegerInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, Integer.parseInt(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSetWrapper, Integer>() {

						@Override
						public void visit(BitSetWrapper k, Integer v) {
							table.put(k, v);
						}
					});
				}
				
			};			
		} else {
				fld = new IndexField() {
				HSearchTableKVInteger table = new HSearchTableKVInteger();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Integer.parseInt(val));
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<Integer, Integer>() {

						@Override
						public void visit(Integer k, Integer v) {
							table.put(k, v);
						}
					});
				}
				
			};
		}
		
		if ( null != exstinData ) fld.append(exstinData);
		
		return fld.index(values);
	}
	
}
