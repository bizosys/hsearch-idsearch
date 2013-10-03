package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.text.ParseException;

import com.bizosys.hsearch.admin.ColGenerator;
import com.bizosys.hsearch.kv.impl.FieldMapping;

public class Initalizer {

	public static void main(String[] args) throws Exception {
    	
    	String schemaPath = "src/test/com/bizosys/hsearch/kv/examresult.xml";
    	String valueObjectPath = "src/test/com/bizosys/hsearch/kv/";
    	String valueObjectClassName = "com.bizosys.hsearch.kv.ExamResult";
    	FieldMapping fm = FieldMapping.getInstance(schemaPath);
		//initalize( create hbase table,create data and index)
    	initalize(fm, schemaPath);
    	//create VO(Value Object) for the schema.
    	ColGenerator.createVO(fm, valueObjectPath, valueObjectClassName);
    }
	
	public static void initalize(final FieldMapping fm, final String schemaPath){
    	try {
	    	loadNindex(fm, schemaPath);
		} catch (Exception e) {
			e.printStackTrace(System.err);
			System.err.println("Error initalizing " + e.getMessage());
		}
	}
    
    public static void loadNindex(final FieldMapping fm, final String schemaPath) throws InterruptedException, ParseException{
    	
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

		String sex = null;
		StringBuilder sb = new StringBuilder(65535);
		
		char FIELD_SEPARATOR = fm.fieldSeparator;
		char RECORD_SEPARATOR = '\n';
		
		for ( int i=0; i<100; i++) {
			
			 sex = ( i%2 == 0) ? "Male" : "Female";
	    	sb.append(classz[classzCounter]).append(FIELD_SEPARATOR).append(i).append(FIELD_SEPARATOR).append(ages[agesCounter])
	    	.append(FIELD_SEPARATOR).append(role[roleCounter]).append(FIELD_SEPARATOR).append(location[locationCounter])
	    	.append(FIELD_SEPARATOR).append((float)i/10).append(FIELD_SEPARATOR).append(remarks[remarksCounter])
	    	.append(FIELD_SEPARATOR).append(comments[commentsCounter]).append(FIELD_SEPARATOR).append(sex.toString()).
	    	append(FIELD_SEPARATOR).append(comments[commentsCounter]).append(RECORD_SEPARATOR);

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
		
		KVIndexerLocal indexer = new KVIndexerLocal();
		try {
			indexer.index(sb.toString(), schemaPath, false);
		} catch (IOException e) {
			e.printStackTrace(System.err);
			System.err.println("Problem in indexing " + e.getMessage());
		}
		
	}
}
