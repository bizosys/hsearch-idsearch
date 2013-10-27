package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVByteInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVByte;
import com.bizosys.hsearch.treetable.Cell2Visitor;

public class IndexFieldByte {

	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook(values, isRepetable, isCompressed);
	}
	
	public static byte[] cook(Iterable<Text> values, byte[] exstingData, 
		final boolean isRepetable, final boolean isCompressed) throws IOException {
		
		IndexField fld = null;

		if ( isRepetable ) {
		
			fld = new IndexField() {
				HSearchTableKVByteInverted table = new HSearchTableKVByteInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, Byte.parseByte(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSetWrapper, Byte>() {

						@Override
						public void visit(BitSetWrapper k, Byte v) {
							table.put(k, v);
						}
					});
				}
				
			};			
		} else {
			fld = new IndexField() {
			
				HSearchTableKVByte table = new HSearchTableKVByte();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Byte.parseByte(val));
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<Integer, Byte>() {

						@Override
						public void visit(Integer k, Byte v) {
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
