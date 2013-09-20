package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVByteInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVByte;

public class IndexFieldByte {
	
	public static byte[] cook(Iterable<Text> values, boolean isRepetable) throws IOException {
		IndexField fld = null;
		if ( isRepetable ) {
			fld = new IndexField() {
				HSearchTableKVByteInverted table = new HSearchTableKVByteInverted();

				@Override
				public void add(int key, String val) {
					table.put(key, Byte.parseByte(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
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
			};
		}
		
		return fld.index(values);
	}
}
