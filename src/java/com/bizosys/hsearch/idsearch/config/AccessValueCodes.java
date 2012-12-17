package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;

public class AccessValueCodes extends _MapCodes {
	
	private static AccessValueCodes instance = null;
	public static AccessValueCodes getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new AccessValueCodes(data);
	}
	
	public AccessValueCodes(byte[] data) throws IOException {
		super(data);
	}

}
