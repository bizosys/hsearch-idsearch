package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

public class KVMapperHBase extends TableMapper<Text, Text> {
    	
	KVMapperBase kBase = new KVMapperBase();
	String[] result = null; 
	Map<String, Field> nameWithFields = null;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		kBase.setup(context);
		nameWithFields = kBase.fm.nameWithField;
		result = new String[kBase.fm.nameWithField.size()];
	}

	
    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    	
    	Arrays.fill(result, "");
    	for (KeyValue kv : value.list()) {
    		String q = new String(kv.getQualifier());
    		result[nameWithFields.get(q).sourceSeq] = new String(kv.getValue());
    	}
    	
    	kBase.map(result, context);
    }
}
