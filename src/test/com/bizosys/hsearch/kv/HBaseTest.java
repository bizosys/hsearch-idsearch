package com.bizosys.hsearch.kv;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HColumnDescriptor;

import com.bizosys.hsearch.hbase.HDML;

public class HBaseTest {

	public static void main(String[] args) throws Exception {
		HColumnDescriptor col = new HColumnDescriptor("1");
		java.util.List<HColumnDescriptor> cols = new ArrayList<HColumnDescriptor>();
		cols.add(col);
		
		HDML.create("a", cols);
    }
	
}
