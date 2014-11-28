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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

class KVReducerHBase extends TableReducer<Text, BytesWritable, ImmutableBytesWritable> {

	Set<Integer> neededPositions = null; 
	FieldMapping fm = null;
	KVReducerBase reducerUtil = null;
	byte[] familyB = null;
	StringBuilder keySb = new StringBuilder(64);
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {

		try {
			Configuration conf = context.getConfiguration();
			String path = conf.get(KVIndexer.XML_FILE_PATH);
			fm = FieldMapping.getInstance(path);
			familyB = fm.familyName.getBytes();
			neededPositions = fm.sourceSeqWithField.keySet();
			KVIndexer.FIELD_SEPARATOR = fm.fieldSeparator;
			
			reducerUtil = new KVReducerBase();
		} catch (ParseException e) {
			e.printStackTrace();
			throw new IOException(e);
		}
	}	


	@Override
	protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {

		String rowKey = key.toString();
		StructureKey structureKey = StructureKey.parseKey(rowKey);
	
		int sourceSeq = Integer.parseInt(structureKey.fieldName);
		Field fld = this.fm.sourceSeqWithField.get(sourceSeq);
		String dataType = fld.getDataType();
		
		int dataFldLen = ( null == structureKey.valueField) ? 0 : structureKey.valueField.length();
		if(dataFldLen > 1 ){
			dataType = "text";
		}
		
		char dataTypeChar = KVIndexer.dataTypesPrimitives.get(dataType);

		structureKey.fieldName = fld.name;
		
		byte[] finalData = null;
		int size = 0;
        try {

			finalData = reducerUtil.cookBytes(structureKey, values, fld, dataTypeChar);
			size = (null == finalData) ? 0 : finalData.length;
			if( 0 == size) return;			
			Put put = new Put(structureKey.getKey(keySb).getBytes());
			put.add(familyB , KVIndexer.COL_NAME, finalData);
			context.write(null, put);
			
		} catch (NumberFormatException ex) {
			String fldDetails = (null == fld) ? "Blank Field" : 
				(" Field Name : " + fld.name + "\t" + "Data Type : " + fld.dataType );
 
			String errMsg = "Error putting data for rowkey :[" + rowKey + "] and datalength : " 
					+ size + " because - " + ex.getMessage() + "\n" + fldDetails;
			
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

			ex.printStackTrace();
			throw new IOException("Error at Reducer " + errMsg, ex);
		}
	}
}
