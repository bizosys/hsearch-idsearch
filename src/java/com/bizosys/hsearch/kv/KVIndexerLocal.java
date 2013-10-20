package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.KVMapperLocal;
import com.bizosys.hsearch.kv.impl.KVMapperLocal.LocalMapContext;
import com.bizosys.hsearch.kv.impl.KVReducerLocal;
import com.bizosys.hsearch.kv.impl.KVReducerLocal.LocalReduceContext;
import com.bizosys.hsearch.util.LineReaderUtil;

public class KVIndexerLocal {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

	}
	
    public void createTable(final String tableName, final String family){
		try {
			List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
			HColumnDescriptor cols = new HColumnDescriptor(family.getBytes());
			colFamilies.add(cols);
			HDML.create(tableName, colFamilies);
		} catch (HBaseException e) {
			e.printStackTrace();
		}
    }
    	
	
    public void index(String data, String schemaPath, boolean skipHeader) throws IOException, InterruptedException, ParseException {

    	FieldMapping fm = FieldMapping.getInstance(schemaPath);

    	//create table in hbase
		HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
    	if ( !admin.tableExists(fm.tableName))
    		createTable(fm.tableName, fm.familyName);
    	
		
		KVMapperLocal mapper = new KVMapperLocal();
		LocalMapContext ctxM = mapper.getContext();
		ctxM.getConfiguration().set(KVIndexer.XML_FILE_PATH, schemaPath);
		mapper.setupRouter(ctxM);

		List<String> records = new ArrayList<String>();
		LineReaderUtil.fastSplit(records, data, '\n');
		
		long i = 1;
		for (String reacord : records) {
			if ( skipHeader ) {
				skipHeader = false;
				continue;
			}
			mapper.mapRouter(new LongWritable(i++), new Text(reacord), ctxM);
		}
		
		KVReducerLocal reducer = new KVReducerLocal();
		LocalReduceContext ctxR = reducer.getContext();
		ctxR.getConfiguration().set(KVIndexer.XML_FILE_PATH, schemaPath);
		reducer.setupRouter(ctxR);
		
		for (String key : ctxM.values.keySet()) {
			reducer.reduceRouter(new Text(key), ctxM.values.get(key), ctxR);
		}
		
		System.out.println("Data is uploaded sucessfully");
    }	

}