package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DocumentTypeCodes extends _MapCodes {
	
	private static DocumentTypeCodes instance = null;
	public static DocumentTypeCodes getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new DocumentTypeCodes(data);
	}
	
	public DocumentTypeCodes(byte[] data) throws IOException {
		super(data);
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, Integer> types = new HashMap<String, Integer>();
		types.put("employee", 1);
		types.put("leave", 2);
		types.put("customer", 3);
		
		DocumentTypeCodes.instanciate( DocumentTypeCodes.builder().add(types).toBytes() );
		System.out.println ( DocumentTypeCodes.getInstance().getCode("customer") );
		
	}	

}
