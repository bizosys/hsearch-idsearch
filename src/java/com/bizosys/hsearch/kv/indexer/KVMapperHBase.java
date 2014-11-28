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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

public class KVMapperHBase extends TableMapper<Text, Text> {
    	
	KVMapperBase kBase = null;
	String[] result = null; 
	Map<String, Field> sourceNameWithFields = null;
	Map<String, Field> nameWithFields = null;
	
	@Override
	protected void setup(Context context) throws IOException,InterruptedException {
		kBase = new KVMapperBase();
		kBase.setup(context);
		nameWithFields = kBase.fm.nameWithField;
		sourceNameWithFields = new HashMap<String, Field>();
		
		int maxSourceSeq = kBase.fm.nameWithField.size() - 1;
		
		for ( Field fld : kBase.fm.nameWithField.values()) {
			if ( fld.sourceSeq < 0) throw new IOException("Invalid Field Configuration : " + fld.name);
			if ( maxSourceSeq < fld.sourceSeq) maxSourceSeq = fld.sourceSeq;
			sourceNameWithFields.put(fld.sourceName, fld);
		}
		
		if(-1 == kBase.fm.internalKey){
			result = new String[maxSourceSeq+1];
		}
		else {
			result = new String[maxSourceSeq + 1 + 2];
		}
	}

	
    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    	
    	byte[] rowId = value.getRow();
    	if ( null == rowId) return;
    	if ( 0 == rowId.length) return;
    	
    	Arrays.fill(result, "");
    	
    	String qualifier = null;
    	Field fld = null;
    	
    	try {
			for (KeyValue kv : value.list()) {
				byte[] qualifierB = kv.getQualifier();
				int qualifierLen = ( null == qualifierB) ? 0 : qualifierB.length;
				if ( qualifierLen == 0 ) continue;
				
				qualifier = new String(qualifierB);
				byte[] valB = kv.getValue();
				String val = null;
				
				if(-1 != kBase.fm.internalKey){
					if(KVIndexer.INTERNAL_KEY.equalsIgnoreCase(qualifier)){
						val = new String(valB);
						result[kBase.fm.internalKey] = val;
						continue;
					}

					if(KVIndexer.PARTITION_KEY.equalsIgnoreCase(qualifier)){
						val = new String(valB);
						result[kBase.fm.internalKey + 1] = val;
						continue;
					}					
				}
				
				fld = sourceNameWithFields.get(qualifier);
				if ( null == fld) continue;

				int len = ( null == valB) ? 0 : valB.length;
				if ( len > 0 ) {
					val = new String(valB);
				}
				
				result[fld.sourceSeq] = val;
			}
			
			kBase.map(result, context);
			

		}  catch (IOException e) {
			
			String msg = "Error while processing for Row = [" + new String(rowId) + "]";
			throw new IOException(msg, e);
			
		} catch (Exception e) {
			String msg = "Failed while processing qualifier [" ;
			if ( null != qualifier) msg = msg +  qualifier;
			msg = msg + "] And Field[";
			
			if ( null != fld) msg = msg +  (fld.toString());
			msg = msg +  "] And Row = [" + new String(rowId) + "]";
			throw new IOException(msg, e);    		
		}
    }
}
