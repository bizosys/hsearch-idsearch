package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bizosys.hsearch.kv.KVIndexer;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVMapper extends Mapper<LongWritable, Text, Text, Text> {
    	
	private KVMapperBase kBase = new KVMapperBase();
	String[] result = null;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		kBase.setup(context);
	}
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	if ( null == result) {
    		ArrayList<String> resultL = new ArrayList<String>();
    		LineReaderUtil.fastSplit(resultL, value.toString(), KVIndexer.FIELD_SEPARATOR);
    		result = new String[resultL.size()];
    	}
    	Arrays.fill(result, null);

    	LineReaderUtil.fastSplit(result, value.toString(), KVIndexer.FIELD_SEPARATOR);
    	
    	kBase.map(result, context);
    	
    }
}
