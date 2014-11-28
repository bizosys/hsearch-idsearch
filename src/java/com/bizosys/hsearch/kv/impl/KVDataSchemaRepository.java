/*
* Copyright 2013 Bizosys Technologies Limited
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
package com.bizosys.hsearch.kv.impl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

public class KVDataSchemaRepository {
	
	
	private static KVDataSchemaRepository colMapper = null; 
	public static KVDataSchemaRepository getInstance() {
		if ( null != colMapper) return colMapper;
		colMapper = new KVDataSchemaRepository();
		return colMapper;
	}
	
	/**
	 * Contains the different mapping containers for the schema fields.
	 * @author shubhendu
	 *
	 */
	public static class KVDataSchema {
		
		/**
		 * Contains mapping of field name and the fields sequence number.
		 */
		public Map<String, Integer> nameToSeqMapping = new HashMap<String, Integer>();
		/**
		 * Contains mapping of fields sequence number and fields name. 
		 */
		public Map<Integer, String> seqToNameMapping = new HashMap<Integer, String>();
		/**
		 * Contains mapping of field and fields data type.
		 */
		public Map<String, Integer> fldWithDataTypeMapping = new HashMap<String, Integer>(); 
		
		public FieldMapping fm = null;
    	static Map<String, Character> dataTypesPrimitives = new HashMap<String, Character>();    	
    	static {
    		dataTypesPrimitives.put("string", 't');
    		dataTypesPrimitives.put("int", 'i');
    		dataTypesPrimitives.put("float", 'f');
    		dataTypesPrimitives.put("double", 'd');
    		dataTypesPrimitives.put("long", 'l');
    		dataTypesPrimitives.put("short", 's');
    		dataTypesPrimitives.put("boolean", 'b');
    		dataTypesPrimitives.put("byte", 'c');
    	}

    	/**
    	 * Populates  the containers based on FieldMapping
    	 * @param fm
    	 */
		public KVDataSchema(final FieldMapping fm) {
			this.fm = fm;
			String dataType = ""; 
			for (String fldName : fm.nameWithField.keySet()) {
				Field fld = fm.nameWithField.get(fldName);
				if ( fld.isStored) {
					nameToSeqMapping.put(fld.name, fld.sourceSeq);
					seqToNameMapping.put(fld.sourceSeq, fld.name);
				}
				
				dataType = fld.getDataType().toLowerCase();
				char dataTypeChar = dataTypesPrimitives.get(dataType);
				int dataTypeField = -1;
				switch (dataTypeChar) {
				case 't':
					
					if(fld.isDocIndex && !fld.isStored)
						dataTypeField = Datatype.FREQUENCY_INDEX;
					else 
						dataTypeField = Datatype.STRING;
					
					break;
				case 'i':
					dataTypeField = Datatype.INTEGER;
					break;
				case 'f':
					dataTypeField = Datatype.FLOAT;
					break;
				case 'd':
					dataTypeField = Datatype.DOUBLE;
					break;
				case 'l':
					dataTypeField = Datatype.LONG;
					break;
				case 's':
					dataTypeField = Datatype.SHORT;
					break;
				case 'b':
					dataTypeField = Datatype.BOOLEAN;
					break;
				case 'c':
					dataTypeField = Datatype.BYTE;
					break;
				default:
					break;
				}
				
				fldWithDataTypeMapping.put(fld.name, dataTypeField);
			}
		}
		
		public static byte getDataTypeMapping(FieldMapping.Field fld) {
			
			String dataType;
			dataType = fld.getDataType().toLowerCase();
			char dataTypeChar = dataTypesPrimitives.get(dataType);
			byte dataTypeField = -1;
			switch (dataTypeChar) {
			case 't':
				
				if(fld.isDocIndex && !fld.isStored)
					dataTypeField = Datatype.FREQUENCY_INDEX;
				else 
					dataTypeField = Datatype.STRING;
				
				break;
			case 'i':
				dataTypeField = Datatype.INTEGER;
				break;
			case 'f':
				dataTypeField = Datatype.FLOAT;
				break;
			case 'd':
				dataTypeField = Datatype.DOUBLE;
				break;
			case 'l':
				dataTypeField = Datatype.LONG;
				break;
			case 's':
				dataTypeField = Datatype.SHORT;
				break;
			case 'b':
				dataTypeField = Datatype.BOOLEAN;
				break;
			case 'c':
				dataTypeField = Datatype.BYTE;
				break;
			default:
				break;
			}
			return dataTypeField;
		}

		/**
		 * Returns whether the field is repeatable.
		 * @param fldName
		 * @return
		 * @throws IOException
		 */
		public final boolean getRepetation(String fldName) throws IOException {
			if ( null == this.fm ) throw new IOException("FieldMapping is not initialized"); 
			return this.fm.nameWithField.get(fldName).isRepeatable;
		}
		
	}
	
	Map<String, KVDataSchema> repositoryMap = new HashMap<String, KVDataSchemaRepository.KVDataSchema>();
	
	/**
	 * Adds KVDataSchema in the repository 
	 * @param repositoryName
	 * @param fm
	 */
	public final void add(final String repositoryName, final FieldMapping fm) {
		if(repositoryMap.containsKey(repositoryName))
			return;
		repositoryMap.put(repositoryName, new KVDataSchema(fm));
	}
	/**
	 * Returns the KVDataSchema for a given repository name.
	 * @param repositoryName
	 * @return
	 */
	public final KVDataSchema get(final String repositoryName) {
		return repositoryMap.get(repositoryName);
	}
	
}
