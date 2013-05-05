/*
* Copyright 2010 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
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
