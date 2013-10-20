package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.util.BitSet;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVLongInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVLong;
import com.bizosys.hsearch.treetable.Cell2Visitor;


public class IndexFieldLong {
	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
		return cook(values, null, isRepetable, isCompressed);
	}

	public static byte[] cook(Iterable<Text> values, final byte[] exstingData, 
		final boolean isRepetable, final boolean isCompressed) throws IOException {
		
		IndexField fld = null;
		
		if ( isRepetable ) {
		
			fld = new IndexField() {
				
				HSearchTableKVLongInverted table = new HSearchTableKVLongInverted(isCompressed);

				@Override
				public void add(int key, String val) {
					table.put(key, Long.parseLong(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<BitSet, Long>() {

						@Override
						public void visit(BitSet k, Long v) {
							table.put(k, v);
						}
					});
				}
				
			};			
		} else {
				fld = new IndexField() {
					
				HSearchTableKVLong table = new HSearchTableKVLong();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Long.parseLong(val));
				}
	
				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
				
				@Override
				public void append(byte[] data) throws IOException  {
					table.parse(data, new Cell2Visitor<Integer, Long>() {

						@Override
						public void visit(Integer k, Long v) {
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
