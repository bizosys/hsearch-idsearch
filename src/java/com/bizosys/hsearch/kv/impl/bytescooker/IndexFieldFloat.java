package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVFloatInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVFloat;

public class IndexFieldFloat {

	public static byte[] cook(Iterable<Text> values, boolean isRepetable) throws IOException {
		IndexField fld = null;
		if ( isRepetable ) {
			fld = new IndexField() {
				HSearchTableKVFloatInverted table = new HSearchTableKVFloatInverted();

				@Override
				public void add(int key, String val) {
					table.put(key, Float.parseFloat(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
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
			};
		}
		
		return fld.index(values);
	}

}
