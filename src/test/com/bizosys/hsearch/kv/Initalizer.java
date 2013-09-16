package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.bizosys.hsearch.admin.ColGenerator;
import com.bizosys.hsearch.hbase.HBaseException;
import com.bizosys.hsearch.hbase.HBaseFacade;
import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.kv.impl.FieldMapping;

public class Initalizer {

	public static final char RECORD_SEPARATOR = '\n';
	public static final char FIELD_SEPARATOR = '|';
	
	public static void main(String[] args) throws Exception {
    	
    	String schemaPath = "src/test/com/bizosys/hsearch/kv/examresult.xml";
    	String valueObjectPath = "src/test/com/bizosys/hsearch/kv/";
    	String valueObjectClassName = "com.bizosys.hsearch.kv.ExamResult";
    	FieldMapping fm = FieldMapping.getInstance();
		//parse the xml file
		fm.parseXML(schemaPath);
		//initalize( create hbase table,create data and index)
    	initalize(fm);
    	//create VO(Value Object) for the schema.
    	ColGenerator.createVO(fm, valueObjectPath, valueObjectClassName);
    }
	
	public static void initalize(final FieldMapping fm){
    	try {
			//create table in hbase
			HBaseAdmin admin =  HBaseFacade.getInstance().getAdmin();
	    	if ( !admin.tableExists(fm.tableName))
	    		createTable(fm.tableName, fm.familyName);
	    	//load and index data in hbase
	    	loadNindex(fm);
	    	
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.err.println("Error initalizing " + e.getMessage());
		}
	}
    
    public static void createTable(final String tableName, final String family){
		try {
			List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
			HColumnDescriptor cols = new HColumnDescriptor(family.getBytes());
			colFamilies.add(cols);
			HDML.create(tableName, colFamilies);
		} catch (HBaseException e) {
			e.printStackTrace();
		}
    }
    
    public static void loadNindex(final FieldMapping fm){
    	
    	String [] classz = new String[] {"A","B"};
		int classzCounter = 0;
		
		int [] ages = new int[] {22,23,24,25,26};
		int agesCounter = 0;
		
		String [] role = new String[] {"scout","monitor","captain","student"};
		int roleCounter = 0;
		
		String [] location = new String[] {"HSR Layout","Bommanahalli","Hebbal","BTM Layout"};
		int locationCounter = 0;

		String [] remarks = new String[] {"good boy and very authentic","fabulous in study","poor performance since previous time"};
		int remarksCounter = 0;

		String [] comments = new String[] {"He is a good boy and very authentic","Tremendous boy","blazing performance in this test"};
		int commentsCounter = 0;

		StringBuilder sb = new StringBuilder(65535);
		for ( int i=0; i<100; i++) {
			
	    	sb.append(classz[classzCounter]).append(FIELD_SEPARATOR).append(i).append(FIELD_SEPARATOR).append(ages[agesCounter])
	    	.append(FIELD_SEPARATOR).append(role[roleCounter]).append(FIELD_SEPARATOR).append(location[locationCounter])
	    	.append(FIELD_SEPARATOR).append((float)i/10).append(FIELD_SEPARATOR).append(remarks[remarksCounter])
	    	.append(FIELD_SEPARATOR).append(comments[commentsCounter]).append(RECORD_SEPARATOR);

	    	classzCounter++;
	    	if ( classzCounter > 1) classzCounter = 0;

	    	agesCounter++;
	    	if ( agesCounter > 4) agesCounter = 0;
	    	
	    	roleCounter++;
	    	if ( roleCounter > 3) 
	    		roleCounter = 0;

	    	remarksCounter++;
	    	commentsCounter++;
	    	if ( remarksCounter > 2){
	    		remarksCounter = 0;
	    		commentsCounter = 0;
	    	}
	    	
	    	locationCounter++;
	    	if ( locationCounter > 3) locationCounter = 0;
		}
		
		StandaloneKVMapReduce indexer = new StandaloneKVMapReduce();
		try {
			indexer.indexData(sb.toString(), fm);
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.err.println("Problem in indexing " + e.getMessage());
		}
	}
}
