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

import com.bizosys.hsearch.functions.GroupSortedObject.FieldType;

public class Datatype {
	
	public static final byte NONE = -1;
	public static final byte BOOLEAN = 0;
	public static final byte BYTE = 1;
	public static final byte SHORT = 2;
	public static final byte INTEGER = 3;
	public static final byte FLOAT = 4;
	public static final byte LONG = 5;
	public static final byte DOUBLE = 6;
	public static final byte STRING = 7;
	public static final byte FREQUENCY_INDEX = 8;
	public static final byte OBJECT = 9;
	
	public static FieldType getFieldType(int type){
		switch (type) {
		case BOOLEAN:
			return FieldType.BOOLEAN;
		case BYTE:
			return FieldType.BYTE;
		case SHORT:
			return FieldType.SHORT;
		case INTEGER:
			return FieldType.INTEGER;
		case FLOAT:
			return FieldType.FLOAT;
		case LONG:
			return FieldType.LONG;
		case DOUBLE:
			return FieldType.DOUBLE;
		case STRING:
			return FieldType.STRING;
		case OBJECT:
			return FieldType.OBJECT;
		default:
			break;
		}
		return null;
	}
}
