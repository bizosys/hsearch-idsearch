package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVDoubleInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVDouble;

public class IndexFieldDouble {
	public static byte[] cook(Iterable<Text> values, boolean isRepetable) throws IOException {
		IndexField fld = null;
		if ( isRepetable ) {
			fld = new IndexField() {
				HSearchTableKVDoubleInverted table = new HSearchTableKVDoubleInverted();

				@Override
				public void add(int key, String val) {
					table.put(key, Double.parseDouble(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
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
			};
		}
		
		return fld.index(values);
	}

}
