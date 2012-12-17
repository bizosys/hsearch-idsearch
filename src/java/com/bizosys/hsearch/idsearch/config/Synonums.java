package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.List;

public class Synonums extends _MapDetails {
	
	private static Synonums instance = null;
	public static Synonums getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new Synonums(data);
	}
	
	public Synonums(byte[] data) throws IOException {
		super(data);
	}
	
	public List<String> getSynonums(String word) throws IOException {
		return fastSplit( this.getCode(word), ',' );
	}
	
}
