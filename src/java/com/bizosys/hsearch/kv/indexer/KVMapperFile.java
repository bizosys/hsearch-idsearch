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
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.bizosys.hsearch.util.LineReaderUtil;

class KVMapperFile extends Mapper<LongWritable, Text, Text, BytesWritable> {
    	
	private KVMapperBase kBase = new KVMapperBase();
	String[] result = null;
	boolean isSkipHeader = false;
	char currentSeparator = '\t'; 

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		String skipHeader = conf.get(KVIndexer.SKIP_HEADER);
		String rawSeparator = conf.get(KVIndexer.RAW_FILE_SEPATATOR);
		currentSeparator = (rawSeparator.equals("\\t")) ? '\t' : rawSeparator.charAt(0);
		
		if ( null != skipHeader) {
			isSkipHeader = "true".equals(skipHeader);
		}
		kBase.setup(context);
	}
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	if ( isSkipHeader ) {
			isSkipHeader = false;
    		if ( 0 == key.get()) return;
    	}
    	
    	if ( null == result) {
    		ArrayList<String> resultL = new ArrayList<String>();
    		LineReaderUtil.fastSplit(resultL, value.toString(), currentSeparator);
    		result = new String[resultL.size()];
    	}
   
    	Arrays.fill(result, null);
    	
    	LineReaderUtil.fastSplit(result, value.toString(), currentSeparator);
    	kBase.map(result, context);
    	
    }
    
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		kBase.cleanup(context);
	}

}
