package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FieldTypeCodes extends _MapCodes {
	
	private static FieldTypeCodes instance = null;
	public static FieldTypeCodes getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new FieldTypeCodes(data);
	}
	
	public FieldTypeCodes(byte[] data) throws IOException {
		super(data);
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, Integer> types = new HashMap<String, Integer>();
		types.put("name", 1);
		types.put("empid", 2);
		types.put("age", 3);
		
		FieldTypeCodes.instanciate( FieldTypeCodes.builder().add(types).toBytes() );
		System.out.println ( FieldTypeCodes.getInstance().getCode("name") );
		
	}		

}
