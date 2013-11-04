package com.bizosys.hsearch.kv;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.KVPlugin;

public class KVPluginDummy implements KVPlugin {

	@Override
	public void setFieldMapping(FieldMapping fm) {
		System.out.println("PLugin:setFieldMapping");
	}

	@Override
	public boolean map(long incrementalIdSeekPosition, String[] input) {
		System.out.println("PLugin:map\t" + incrementalIdSeekPosition);
		return true;
	}

	@Override
	public boolean reduce(String roKey, char dataTypeChar, int sourceSeq, Iterable<Text> values) {
		
		System.out.println("PLugin:reduce\t" + roKey);
		return true;
	}

}
