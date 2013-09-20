package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVBooleanInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVBoolean;

public class IndexFieldBoolean {

	public static byte[] cook(Iterable<Text> values, boolean isRepetable) throws IOException {
		IndexField fld = null;
		if ( isRepetable ) {
			fld = new IndexField() {
				HSearchTableKVBooleanInverted table = new HSearchTableKVBooleanInverted();

				@Override
				public void add(int key, String val) {
					table.put(key, Boolean.parseBoolean(val));
				}

				@Override
				public byte[] getBytes() throws IOException  {
					return table.toBytes();
				}
			};			
		} else {
				fld = new IndexField() {
				HSearchTableKVBoolean table = new HSearchTableKVBoolean();
	
				@Override
				public void add(int key, String val) {
					table.put(key, Boolean.parseBoolean(val));
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
