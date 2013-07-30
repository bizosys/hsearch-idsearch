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
