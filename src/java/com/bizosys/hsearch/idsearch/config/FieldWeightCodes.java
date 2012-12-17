package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FieldWeightCodes extends _WeightCodes {
	
	private static FieldWeightCodes instance = null;
	public static FieldWeightCodes getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new FieldWeightCodes(data);
	}
	
	public FieldWeightCodes(byte[] data) throws IOException {
		super(data);
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, Byte> weights = new HashMap<String, Byte>();
		weights.put("title", (byte) 100);
		weights.put("subject", (byte) 90);
		weights.put("body", (byte) 25);
		
		FieldWeightCodes.instanciate( FieldWeightCodes.builder().add(weights).toBytes() );
		System.out.println ( FieldWeightCodes.getInstance().getCode("body") );
		
	}

}
