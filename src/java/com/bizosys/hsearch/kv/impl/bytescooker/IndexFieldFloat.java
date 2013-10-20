package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVFloatInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVFloat;
import com.bizosys.hsearch.treetable.Cell2Visitor;


public class IndexFieldFloat {

	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook(values, null, isRepetable, isCompressed);
	}
	
	public static byte[] cook(Iterable<Text> values, final byte[] exstingData, 
		final boolean isRepetable, final boolean isCompressed) throws IOException {

		IndexField fld = null;
		
		if ( isRepetable ) {
		
			fld = new IndexField() {
			
				HSearchTableKVFloatInverted table = new HSearchTableKVFloatInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, Float.parseFloat(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSet, Float>() {

						@Override
						public void visit(BitSet k, Float v) {
							table.put(k, v);
						}
					});
				}
				
			};			
		} else {
			
			fld = new IndexField() {
					
				HSearchTableKVFloat table = new HSearchTableKVFloat();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Float.parseFloat(val));
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<Integer, Float>() {

						@Override
						public void visit(Integer k, Float v) {
							table.put(k, v);
						}
					});
				}
				
			};
		}
		
		if ( null != exstingData ) fld.append(exstingData);
		return fld.index(values);
	}

}
