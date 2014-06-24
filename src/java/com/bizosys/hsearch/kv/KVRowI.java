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
package com.bizosys.hsearch.kv;

import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.TypedObject;

/**
 * 
 * The final result returned for a given search query is collection of instances of this interface.
 *
 */
public interface KVRowI {
	
	/**
	 * Sets the id for a record.
	 * @param id
	 */
	void setId(final Integer id);
	
	/**
	 * 
	 * @return The id for a given record.This is a internal id generated during indexing.
	 */
	int getId();
	
	/**
	 * Sets the mergeid for a given search query.
	 * Mergeid is a combination of mergefields from schema and 
	 * the current field that is queried separated by a underscore.
	 * @param mergeId
	 */
	void setmergeId(final String mergeId);
	
	/**
	 * 
	 * @return The mergeid for a given search query.
	 */
	String getmergeId();
	
	/**
	 * Create a new default instance.
	 * @return a new instance of KVRowI
	 */
	KVRowI create();

	/**
	 * Create a new instance using the parameters from {@link KVDataSchema}dataschema.
	 * @return a new instance of KVRowI
	 */
	KVRowI create(final KVDataSchema dataSchema);
	
	/**
	 * Create a new instance using the parameters from {@link KVDataSchema}dataschema.
	 * And sets the id and mergeid.
	 * @return a new instance of KVRowI
	 */
	KVRowI create(final KVDataSchema dataSchema, final Integer id, final String mergeId);
	
	/**
	 * Sets the value for a given field.
	 * @param fldName
	 * @param value
	 */
	void setValue(final String fldName, final Object value);
	
	/**
	 * Returns the value for a given field.
	 * @param fldName
	 * @return the value for a given field.
	 */
	Object getValue(final String fldName);
	
	/**
	 * Returns {@link ThemeReader} {@link TypedObject} typedObject 
	 * which can be converted to individual types.
	 * @param fldName
	 * @return the value for a given field.
	 */
	TypedObject getValueNative(final String fldName);
	
	/**
	 * Returns the datatype for a given field.
	 * @param fldName
	 * @return the datatype for a given field.
	 */
	int getDataType(final String fldName);
	
	/**
	 * Returns the unique sequence number for a given field.
	 * Same as sequencenumber defined in schema. 
	 * @param fldName
	 * @return the unique sequence number for a given field.
	 */
	int getValueSeq(final String fldName);
	
	/**
	 * 
	 * Returns a value given a field seq.
	 * @param fldSeq
	 * @return a value given a field seq.
	 */
	Object getValue(final int fldSeq);
	
}
