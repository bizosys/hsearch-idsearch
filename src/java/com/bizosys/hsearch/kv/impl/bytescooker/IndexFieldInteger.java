package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVIntegerInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;

public class IndexFieldInteger {
	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
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
			};
		}
		
		return fld.index(values);
	}
}
