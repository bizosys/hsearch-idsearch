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
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.idsearch.util.MapFileUtil;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.indexer.HsearchCounterUtil.HsearchCounters;

class KVReducerMapFile extends Reducer<Text, BytesWritable, NullWritable, Text>{

	public static boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;
	KVReducerBase reducerUtil = null;
	String indexOutputFolder = null;
	String indexPath = null;
	StringBuilder metaData = null;
	StringBuilder keySb = new StringBuilder(64);
	

	MapFileUtil.Writer writer = null;

	IOException setUpException = null;

	@Override
	protected void setup(Context context)throws IOException, InterruptedException {

		try {
			Configuration conf = context.getConfiguration();
			String path = conf.get(KVIndexer.XML_FILE_PATH);
			fm = FieldMapping.getInstance(path);
			neededPositions = fm.sourceSeqWithField.keySet();
			KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;

			reducerUtil = new KVReducerBase();
			indexOutputFolder = conf.get(KVIndexer.OUTPUT_FOLDER);
			indexPath = indexOutputFolder + "-" + context.getTaskAttemptID().getTaskID().getId();
			metaData = new StringBuilder(1024);

			writer = new MapFileUtil.Writer();
			writer.setConfiguration(conf);
			writer.open(Text.class, BytesWritable.class, indexPath, CompressionType.NONE);

		} catch (ParseException e) {
			e.printStackTrace();
			setUpException = new IOException(e);
		}
	}

	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values,Context context)
			throws IOException, InterruptedException {

		if ( null != setUpException) throw setUpException;
		long start = System.currentTimeMillis();
		String rowKey = key.toString();
		StructureKey structureKey = StructureKey.parseKey(rowKey);

		int sourceSeq = Integer.parseInt(structureKey.fieldName);
		Field fld = this.fm.sourceSeqWithField.get(sourceSeq);
		String dataType = fld.getDataType();

		int dataFldLen = ( null == structureKey.valueField) ? 0 : structureKey.valueField.length();
		if(dataFldLen > 1 ){
			dataType = "text";
		}

		char dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType.toLowerCase());

		byte[] finalData = null;
		int size = 0;

		try {
			
			finalData = reducerUtil.cookBytes(structureKey, values, fld, dataTypeChar);
			size = ( null == finalData ) ? 0 : finalData.length;
			if(0 == size) return;
			
			structureKey.fieldName = fld.name;
			String newKey = structureKey.getKey(keySb);
			context.setStatus("Key = " + newKey  + " ValueSize = " + size/1000000 + " Mb");
			
			Text mapKey = new Text(newKey);
			writer.append(mapKey, new BytesWritable(finalData));

			long end = System.currentTimeMillis();
			long timeTaken = (end - start);
			context.getCounter(HsearchCounters.Index_Reduce_Time).increment(timeTaken);

		} catch (Exception e) {
			String fldDetails = (null == fld) ? "Blank Field" : 
				(" Field Name : " + fld.name + "\t" + "Data Type : " + fld.dataType );
 
			String errMsg = "Error putting data for rowkey :[" + rowKey + "] and datalength : " 
					+ size + " because - " + e.getMessage() + "\n" + fldDetails;
			
			System.err.println(errMsg);
			boolean isFirst = true;
			int i = 0;
			System.err.println("Few values in this bucket are : " );
			for (BytesWritable b : values) {
				i++;
				if( isFirst ) isFirst = false;
				else System.err.print(" | ");
				byte[] data = b.getBytes();
				if ( null == data) System.err.print("Null bytes");
				else System.err.print(new String(data));
				if(i>10)break;
			}

			e.printStackTrace();
			throw new IOException("Error at Reducer " + errMsg, e);
		}

	}
	@Override
	protected void cleanup(Context context)throws IOException, InterruptedException {
		super.cleanup(context);
		if(null != writer) writer.close();
	}
	
}
