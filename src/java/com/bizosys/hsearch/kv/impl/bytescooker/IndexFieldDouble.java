package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVDoubleInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVDouble;
import com.bizosys.hsearch.treetable.Cell2Visitor;


public class IndexFieldDouble {

	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook( values, null, isRepetable, isCompressed);
	}
	
	public static byte[] cook(Iterable<Text> values, final byte[] exstingData, 
		final boolean isRepetable, final boolean isCompressed) throws IOException {
	
		IndexField fld = null;
		
		if ( isRepetable ) {
		
			fld = new IndexField() {
			
				HSearchTableKVDoubleInverted table = new HSearchTableKVDoubleInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, Double.parseDouble(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSet, Double>() {

						@Override
						public void visit(BitSet k, Double v) {
							table.put(k, v);
						}
					});
				}
				
			};			
		} else {
			fld = new IndexField() {
			
				HSearchTableKVDouble table = new HSearchTableKVDouble();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Double.parseDouble(val));
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<Integer, Double>() {

						@Override
						public void visit(Integer k, Double v) {
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