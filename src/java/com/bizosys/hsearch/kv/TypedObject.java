package com.bizosys.hsearch.kv;

import com.bizosys.hsearch.kv.impl.Datatype;

public class TypedObject {

	public boolean valBoolean = false;
	public byte valByte = 0;
	public short valShort = 0;
	public int valInt = 0;
	public float valFloat = 0.0f;
	public long valLong = 0l;
	public double valDouble = 0.0;
	public String valStr = null;

	int type = Datatype.NONE;

	public TypedObject(final boolean value) {
		this.valBoolean = value;
		this.type = Datatype.BOOLEAN;
	}

	public TypedObject(final byte value) {
		this.valByte = value;
		this.type = Datatype.BYTE;
	}

	public TypedObject(final short value) {
		this.valShort = value;
		this.type = Datatype.SHORT;
	}

	public TypedObject(final int value) {
		this.valInt = value;
		this.type = Datatype.INTEGER;
	}

	public TypedObject(final float value) {
		this.valFloat = value;
		this.type = Datatype.FLOAT;
	}

	public TypedObject(final long value) {
		this.valLong = value;
		this.type = Datatype.LONG;
	}

	public TypedObject(final double value) {
		this.valDouble = value;
		this.type = Datatype.LONG;
	}

	public TypedObject(final String value) {
		this.valStr = value;
		this.type = Datatype.STRING;
	}

	public boolean equals(TypedObject other) {
		switch ( type ) {
			case Datatype.BOOLEAN:
				return (this.valBoolean == other.valBoolean);
				
			case Datatype.BYTE:
				return (this.valByte == other.valByte);
				
			case Datatype.SHORT:
				return (this.valShort == other.valShort);
			
			case Datatype.INTEGER:
				return (this.valInt == other.valInt);

			case Datatype.FLOAT:
				return (this.valFloat == other.valFloat);
			
			case Datatype.LONG:
				return (this.valLong == other.valLong);
			
			case Datatype.DOUBLE:
				return (this.valDouble == other.valDouble);
			
			case Datatype.STRING:
				int compareValue = this.valStr.compareTo(other.valStr);
				if(0 == compareValue)return true;
				else return false;
		}
		return false;
	}
	
	@Override
	public String toString() {
		switch ( type ) {
		case Datatype.BOOLEAN:
			return ((Boolean)this.valBoolean).toString();
			
		case Datatype.BYTE:
			return ((Byte)this.valByte).toString();
			
		case Datatype.SHORT:
			return ((Short)this.valShort).toString();
		
		case Datatype.INTEGER:
			return ((Integer)this.valInt).toString();

		case Datatype.FLOAT:
			return ((Float)this.valFloat).toString();
		
		case Datatype.LONG:
			return ((Long)this.valLong).toString();
		
		case Datatype.DOUBLE:
			return ((Double)this.valDouble).toString();
		
		case Datatype.STRING:
			return this.valStr;
		}
		return null;
	}
}
