/**
*    Copyright 2014, Bizosys Technologies Pvt Ltd
*
*    This software and all information contained herein is the property
*    of Bizosys Technologies.  Much of this information including ideas,
*    concepts, formulas, processes, data, know-how, techniques, and
*    the like, found herein is considered proprietary to Bizosys
*    Technologies, and may be covered by U.S., India and foreign patents or
*    patents pending, or protected under trade secret laws.
*    Any dissemination, disclosure, use, or reproduction of this
*    material for any reason inconsistent with the express purpose for
*    which it has been disclosed is strictly forbidden.
*
*                        Restricted Rights Legend
*                        ------------------------
*
*    Use, duplication, or disclosure by the Government is subject to
*    restrictions as set forth in paragraph (b)(3)(B) of the Rights in
*    Technical Data and Computer Software clause in DAR 7-104.9(a).
*/

package com.bizosys.hsearch.kv.indexer;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bizosys.hsearch.kv.impl.FieldMapping;

class KVMapperHFile extends Mapper<Text, BytesWritable, ImmutableBytesWritable, KeyValue>{

	
	FieldMapping fm = null;
	byte[] familyName = null;
	byte[] qualifier = new byte[]{0};

	ImmutableBytesWritable hKey = new ImmutableBytesWritable();

	@Override
	protected void setup(Context context)throws IOException, InterruptedException {

		try {
			
			Configuration conf = context.getConfiguration();
			String path = conf.get(KVIndexer.XML_FILE_PATH);
			fm = FieldMapping.getInstance(path);
			familyName = fm.familyName.getBytes();
			
		} catch (ParseException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}


	@Override
	protected void map(Text key, BytesWritable value,Context context) throws IOException, InterruptedException{

		String rowKey = key.toString();
		byte[] rowData = new byte[value.getLength()];
		System.arraycopy(value.getBytes(), 0, rowData, 0, value.getLength());
		hKey.set(rowKey.getBytes());
		KeyValue kv = new KeyValue(hKey.get(), familyName, qualifier, rowData);
		context.write(hKey, kv);
		
	}
}
