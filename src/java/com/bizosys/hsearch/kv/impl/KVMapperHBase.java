package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.kv.KVIndexer;
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
		result = new String[maxSourceSeq+1];
	}

	
    @Override
    protected void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    	
		/**
    	for (KeyValue kv : value.list()) {
			System.out.println(new String(value.getRow()) + "\t" + new String(kv.getQualifier()) + "\t" + kv.getValueLength());
		}
		*/

    	byte[] rowId = value.getRow();
    	if ( null == rowId) return;
    	if ( 0 == rowId.length) return;
    	
    	Arrays.fill(result, "");
    	
    	String qualifier = null;
    	Field fld = null;
    	
    	Exception failure = null;
    	
    	try {
    		if ( sourceNameWithFields.containsKey("ROWID")) {
    			Field rowFld = sourceNameWithFields.get("ROWID");
    			char rowFldDatatype = KVIndexer.dataTypesPrimitives.get(rowFld.getDataType());
				Object finalData = null;
				switch (rowFldDatatype) {
				
				case 't':
				case 'e':
					finalData = new String(rowId);
					break;

				case 'i':
					if ( rowId.length != 4) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData =  Storable.getInt(0, rowId);
					break;

				case 'f':
					if ( rowId.length != 4) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData = Storable.getFloat(0, rowId);
					break;

				case 'l':
					if ( rowId.length != 8) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData = Storable.getLong(0, rowId);
					break;

				case 'd':
					if ( rowId.length != 8) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData = Storable.getDouble(0, rowId);
					break;
					
				case 's':
					if ( rowId.length != 2) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData = Storable.getShort(0, rowId);
					break;

				case 'b':
					if ( rowId.length != 1) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData = (rowId[0] == 1);
					break;

				case 'c':
					if ( rowId.length != 1) {
						System.err.println("Skipping bad Id :" + new String(rowId));
						return;
					}
					finalData = new Byte( (byte) rowId[0] ).toString() ;
					break;

				default:
					break;
				}  
				
				if ( null == finalData) return;
    			result[rowFld.sourceSeq] = finalData.toString();
    		}
    		
			for (KeyValue kv : value.list()) {
				byte[] qualifierB = kv.getQualifier();
				int qualifierLen = ( null == qualifierB) ? 0 : qualifierB.length;
				if ( qualifierLen == 0 ) continue;
				
				qualifier = new String(kv.getQualifier());
				byte[] valB = kv.getValue();
				String val = null;
				fld = sourceNameWithFields.get(qualifier);
				if ( null == fld) continue;
				
				int len = ( null == valB) ? 0 : valB.length;
				if ( 0 == len) {
					if ( fld.skipNull) continue;
					val = fld.defaultValue; 
				} else {
					char type = KVIndexer.dataTypesPrimitives.get(fld.getDataType().toLowerCase());
					switch ( type) {
						case 't':
							val = new String(valB);
							break;
						case 'i':
							val = new Integer(Storable.getInt(0, valB)).toString();
							break;
						case 's':
							val = new Short(Storable.getShort(0, valB)).toString();
							break;
						case 'f':
							val = new Float(Storable.getFloat(0, valB)).toString();
							break;
						case 'd':
							val = new Double(Storable.getDouble(0, valB)).toString();
							break;
						case 'l':
							val = new Long(Storable.getLong(0, valB)).toString();
							break;
						case 'b':
							val = (valB[0] == 1) ? "true" : "false";
							break;
						case 'c':
							if ( valB.length == 1) {
								val = new Byte( (byte) valB[0] ).toString() ;
							} else {
								val = new Byte(new String(valB)).toString();
							}
							
							break;
						default:
							val = new String(valB);
					}
				}
				
				result[sourceNameWithFields.get(qualifier).sourceSeq] = val;
			}
			
			kBase.map(result, context);

		} catch (NullPointerException e) {
			failure = e;
		} catch (Exception e) {
			failure = e;
		}
    	
    	if ( null != failure) {
			String msg = "Failed while processing qualifier [" ;
			if ( null != qualifier) msg = msg +  qualifier;
			msg = msg + "] And Field[";
			
			if ( null != fld) msg = msg +  (fld.name + "\t" + fld.getDataType());
			msg = msg +  "] And Row = [" + new String(rowId) + "]";

			throw new IOException(msg, failure);    		
    	}
    }
}
