package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;

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

}
