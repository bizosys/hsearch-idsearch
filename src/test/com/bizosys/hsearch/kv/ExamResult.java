package com.bizosys.hsearch.kv;

import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;

import com.bizosys.hsearch.kv.impl.TypedObject;
import com.bizosys.hsearch.kv.KVRowI;

import com.bizosys.unstructured.util.IdSearchLog;

public class ExamResult extends GroupSortedObject implements KVRowI{
	
	public String mergeId = null;
	public int id = -1;
	public int empid = 0;
	public String sex = null;
	public String classz = null;
	public float marks = 0.0f;
	public String location = null;
	public int age = 0;
	public String commentsval = null;
	public String role = null;

		
	KVDataSchema dataSchema = null;
	
	public ExamResult() {}
	public ExamResult(KVDataSchema dataScheme) {
		this.dataSchema = dataScheme;
	}

	public final void setValue(final String key, final Object value){
		if ( ! this.dataSchema.nameToSeqMapping.containsKey(key) ) {
			IdSearchLog.l.warn("Warning: Field is not stored type or undefined > " + key);
			return;
		}
		int keySeq = this.dataSchema.nameToSeqMapping.get(key);
		switch ( keySeq ) {
		case 1:
			 this.empid = (Integer)value;
		 break;
		case 8:
			 this.sex = value.toString();
		 break;
		case 0:
			 this.classz = value.toString();
		 break;
		case 5:
			 this.marks = (Float)value;
		 break;
		case 4:
			 this.location = value.toString();
		 break;
		case 2:
			 this.age = (Integer)value;
		 break;
		case 9:
			 this.commentsval = value.toString();
		 break;
		case 3:
			 this.role = value.toString();
		 break;

		}
	}
	
		@Override
	public final Object getValue(final String key){
		if ( ! this.dataSchema.nameToSeqMapping.containsKey(key) ) {
			IdSearchLog.l.warn("Warning: Field is not stored type or undefined > " + key);
			return null;
		}
	
		int keySeq = this.dataSchema.nameToSeqMapping.get(key);
		switch ( keySeq ) {
		case 1:
			 return this.empid;
		case 8:
			 return this.sex;
		case 0:
			 return this.classz;
		case 5:
			 return this.marks;
		case 4:
			 return this.location;
		case 2:
			 return this.age;
		case 9:
			 return this.commentsval;
		case 3:
			 return this.role;

		}
		return null;
	}

	@Override
	public TypedObject getValueNative(String name) {
		int keySeq = this.dataSchema.nameToSeqMapping.get(name);
		switch ( keySeq ) {
		case 1:
			 return new TypedObject(this.empid);
		case 8:
			 return new TypedObject(this.sex);
		case 0:
			 return new TypedObject(this.classz);
		case 5:
			 return new TypedObject(this.marks);
		case 4:
			 return new TypedObject(this.location);
		case 2:
			 return new TypedObject(this.age);
		case 9:
			 return new TypedObject(this.commentsval);
		case 3:
			 return new TypedObject(this.role);

		}
		return null;
	}		
	
	@Override
	public final boolean getBooleanField(final int fldSequence) {
		switch ( fldSequence ) {

		}
		return false;
	}

	@Override
	public final byte getByteField(final int fldSequence) {
		switch ( fldSequence ) {

		}
		return 0;
	}

	@Override
	public final short getShortField(final int fldSequence) {
		switch ( fldSequence ) {

		}
		return 0;
	}

	@Override
	public final int getIntegerField(final int fldSequence) {
		switch ( fldSequence ) {
	case 1:
		 return this.empid;
	case 2:
		 return this.age;

		}
		return 0;
	}

	@Override
	public final float getFloatField(final int fldSequence) {
		switch ( fldSequence ) {
	case 5:
		 return this.marks;

		}
		return 0;
	}

	@Override
	public final double getDoubleField(final int fldSequence) {
		switch ( fldSequence ) {

		}
		return 0;
	}

	@Override
	public final long getLongField(final int fldSequence) {
		switch ( fldSequence ) {

		}
		return 0;
	}

	@Override
	public final String getStringField(final int fldSequence) {
		switch ( fldSequence ) {
	case 8:
		 return this.sex;
	case 0:
		 return this.classz;
	case 4:
		 return this.location;
	case 9:
		 return this.commentsval;
	case 3:
		 return this.role;

		}
		return null;
	}

	@Override
	public final Object getObjectField(final int fldSequence) {
		return null;
	}
	
	@Override
	public final KVRowI create() {
		return new ExamResult();
	}

	@Override
	public final KVRowI create(final KVDataSchema dataSchema) {
		return new ExamResult(dataSchema);
	}


	@Override
	public final int getDataType(final String name) {
		return 0;
	}
	
	@Override
	public final void setId(final Integer id) {
		this.id = id;
	}

	@Override
	public final int getId() {
		return this.id;
	}

	@Override
	public void setmergeId(String mergeId) {
		this.mergeId = mergeId;
	}

	@Override
	public String getmergeId() {
		return this.mergeId;
	}
	

	@Override
	public int getValueSeq(String fldName) {
		return this.dataSchema.nameToSeqMapping.get(fldName);
	}
	
		@Override
	public Object getValue(int fldSeq) {
		switch ( fldSeq ) {
		case 1:
			 return this.empid;
		case 8:
			 return this.sex;
		case 0:
			 return this.classz;
		case 5:
			 return this.marks;
		case 4:
			 return this.location;
		case 2:
			 return this.age;
		case 9:
			 return this.commentsval;
		case 3:
			 return this.role;

		}
		return null;
	}	

}
