package com.bizosys.hsearch.kv.impl;

import org.apache.hadoop.io.Text;

public interface KVPlugin {
	void setFieldMapping(FieldMapping fm);
	boolean map(long incrementalIdSeekPosition, String[] input);
	boolean reduce(String roKey, char dataTypeChar, int sourceSeq, Iterable<Text> values);
}
