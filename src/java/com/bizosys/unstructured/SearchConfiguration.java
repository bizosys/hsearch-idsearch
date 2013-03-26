package com.bizosys.unstructured;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.bizosys.hsearch.idsearch.config.AccessTypeCodes;
import com.bizosys.hsearch.idsearch.config.AccessValueCodes;
import com.bizosys.hsearch.idsearch.config.DocumentTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldTypeCodes;
import com.bizosys.hsearch.idsearch.config.FieldWeightCodes;
import com.bizosys.hsearch.idsearch.config.Stopwords;
import com.bizosys.hsearch.idsearch.config.Synonums;

public class SearchConfiguration {
	
	private static SearchConfiguration singleton = new SearchConfiguration();
	public static SearchConfiguration getInstance() {
		return singleton;
	}
	
	private SearchConfiguration ()  {
	}
	
	/**
	 * ****************************************************************************************************
	 */

	public DocumentTypeCodes instantiateDocumentTypeCodes(Map<String, Integer> types) 
			throws IOException, InstantiationException {
		return instantiateDocumentTypeCodes(setDocumentTypeCodes(types));
	}
	
	public DocumentTypeCodes instantiateDocumentTypeCodes(byte[] data) 
		throws IOException, InstantiationException {
		
		DocumentTypeCodes.instanciate(data);
		return DocumentTypeCodes.getInstance();
	}

	public byte[] setDocumentTypeCodes(Map<String, Integer> types) throws IOException {
		return DocumentTypeCodes.builder().add(types).toBytes();
	}
	
	public DocumentTypeCodes getDocumentTypeCodes() throws InstantiationException {
		if ( null == DocumentTypeCodes.getInstance()) throw new InstantiationException("FieldTypeCodes not initialized."); 
		return DocumentTypeCodes.getInstance();
	}
	

	/**
	 * ****************************************************************************************************
	 */

	public byte[] setAccessTypeCodes(Map<String, Integer> types) throws IOException {
		return AccessTypeCodes.builder().add(types).toBytes();
	}
	
	public AccessTypeCodes instantiateAccessTypeCodes(byte[] data) 
			throws IOException, InstantiationException {
			
		AccessTypeCodes.instanciate(data);
		return AccessTypeCodes.getInstance();
	}

	/**
	 * ****************************************************************************************************
	 */

	public byte[] setAccessValueCodes(Map<String, Integer> types) throws IOException {
		return AccessValueCodes.builder().add(types).toBytes();
	}
	
	public AccessValueCodes instantiateAccessValueCodes(byte[] data) 
			throws IOException, InstantiationException {
			
		AccessValueCodes.instanciate(data);
		return AccessValueCodes.getInstance();
	}

	/**
	 * ****************************************************************************************************
	 */
	
	public FieldTypeCodes instantiateFieldTypeCodes(Map<String, Integer> types) 
			throws IOException, InstantiationException {
		return instantiateFieldTypeCodes(setFieldTypeCodes(types));
	}	

	public byte[] setFieldTypeCodes(Map<String, Integer> types) throws IOException {
		return FieldTypeCodes.builder().add(types).toBytes();
	}
	
	public FieldTypeCodes instantiateFieldTypeCodes(byte[] data) 
			throws IOException, InstantiationException {
			
		FieldTypeCodes.instanciate(data);
		return FieldTypeCodes.getInstance();
	}

	public FieldTypeCodes getFieldTypeCodes() throws InstantiationException {
		if ( null == FieldTypeCodes.getInstance()) throw new InstantiationException("FieldTypeCodes not initialized."); 
		return FieldTypeCodes.getInstance();
	}
	

	/**
	 * ****************************************************************************************************
	 */

	public byte[] setFieldWeightCodes(Map<String, Byte> types) throws IOException {
		return FieldWeightCodes.builder().add(types).toBytes();
	}
	
	public FieldWeightCodes instantiateFieldWeightCodes(byte[] data) 
			throws IOException, InstantiationException {
			
		FieldWeightCodes.instanciate(data);
		return FieldWeightCodes.getInstance();
	}

	/**
	 * ****************************************************************************************************
	 */

	public byte[] setStopwords(Collection<String> words) throws IOException {
		return Stopwords.builder().add(words).toBytes();
	}
	
	public Stopwords instantiateStopwords(byte[] data) throws IOException, InstantiationException {
		return new Stopwords(data);
	}

	/**
	 * ****************************************************************************************************
	 */

	public byte[] setSynonums(Map<String, String> words) throws IOException {
		return Synonums.builder().add(words).toBytes();
	}
	
	public Synonums instantiateSynonums(byte[] data) throws IOException, InstantiationException {
		return new Synonums(data);
	}

}
