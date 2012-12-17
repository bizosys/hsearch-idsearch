package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;

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

}
