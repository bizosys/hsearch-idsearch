package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.KVIndexer;

public abstract class IndexField {
	
	public byte[] index(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;

		for (Text text : values) {
			if ( null == text) continue;
			Arrays.fill(resultValue, null);

			line = text.toString();
			
			int index = line.indexOf(KVIndexer.FIELD_SEPARATOR);
			if (index >= 0) {
				resultValue[0] = line.substring(0, index);
				if (index <= line.length() - 1) resultValue[1] = line.substring(index + 1);
			}
					
			int containerKey = Integer.parseInt(resultValue[0]);
			hasValue = true;
			add(containerKey, resultValue[1]);
		}
		
		if ( hasValue ) {
			finalData = getBytes();
		}
		return finalData;
	}
	
	public abstract void add(int key, String val);
	public abstract byte[] getBytes() throws IOException;	
}
