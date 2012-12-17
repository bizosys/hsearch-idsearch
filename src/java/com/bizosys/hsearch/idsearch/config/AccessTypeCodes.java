package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;

public class AccessTypeCodes extends _MapCodes {
	
	private static AccessTypeCodes instance = null;
	public static AccessTypeCodes getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new AccessTypeCodes(data);
	}
	
	public AccessTypeCodes(byte[] data) throws IOException {
		super(data);
	}

}
