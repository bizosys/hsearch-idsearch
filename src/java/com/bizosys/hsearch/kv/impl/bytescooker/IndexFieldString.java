package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVStringInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVString;

public class IndexFieldString {
	public static byte[] cook(Iterable<Text> values, boolean isRepetable) throws IOException {
		IndexField fld = null;
		if ( isRepetable ) {
			fld = new IndexField() {
				HSearchTableKVStringInverted table = new HSearchTableKVStringInverted();

				@Override
				public void add(int key, String val) {
					table.put(key, val);
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
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
			};
		}
		
		return fld.index(values);
	}
}
