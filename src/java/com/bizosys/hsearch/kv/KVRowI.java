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

public interface KVRowI {

	void setId(final Integer id);
	int getId();
	void setmergeId(final String mergeId);
	String getmergeId();
	KVRowI create();
	KVRowI create(final KVDataSchema dataSchema);
	KVRowI create(final KVDataSchema dataSchema, final Integer id, final String mergeId);
	void setValue(final String fldName, final Object value);
	Object getValue(final String fldName);
	TypedObject getValueNative(final String fldName);
	int getDataType(final String fldName);
	
	int getValueSeq(final String fldName);
	Object getValue(final int fldSeq);
	
}
