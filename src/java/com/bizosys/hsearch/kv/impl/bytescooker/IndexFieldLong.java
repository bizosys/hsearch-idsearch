package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVLongInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVLong;

public class IndexFieldLong {
	public static byte[] cook(Iterable<Text> values, final boolean isRepetable, final boolean isCompressed) throws IOException {
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
			};
		}
		
		return fld.index(values);
	}
}
