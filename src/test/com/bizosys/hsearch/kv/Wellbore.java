package com.bizosys.hsearch.kv;

import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;

import com.bizosys.hsearch.kv.impl.TypedObject;
import com.bizosys.hsearch.kv.KVRowI;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.google.gson.annotations.Expose;


public class Wellbore extends GroupSortedObject implements KVRowI{
	
	public String __mergeId__ = null;
	public int __id__ = -1;
	@Expose public long timestampordepth = 0L;
	@Expose public String category = null;
	@Expose public int readingtype = 0;
	@Expose public int welboreid = 0;
	@Expose public int readingvalue = 0;

		
	KVDataSchema dataSchema = null;
	
	public Wellbore() {}
	public Wellbore(KVDataSchema dataScheme) {
		this.dataSchema = dataScheme;
	}

	public final void setValue(final String key, final Object value){
		if ( ! this.dataSchema.nameToSeqMapping.containsKey(key) ) {
			IdSearchLog.l.warn("Warning: Field is not stored type or undefined > " + key);
			return;
		}
		int keySeq = this.dataSchema.nameToSeqMapping.get(key);
		switch ( keySeq ) {
		case 3:
			 this.timestampordepth = (Long)value;
		 break;
		case 1:
			 this.category = value.toString();
		 break;
		case 2:
			 this.readingtype = (Integer)value;
		 break;
		case 0:
			 this.welboreid = (Integer)value;
		 break;
		case 4:
			 this.readingvalue = (Integer)value;
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
		case 3:
			 return this.timestampordepth;
		case 1:
			 return this.category;
		case 2:
			 return this.readingtype;
		case 0:
			 return this.welboreid;
		case 4:
			 return this.readingvalue;

		}
		return null;
	}

	@Override
	public TypedObject getValueNative(String name) {
		int keySeq = this.dataSchema.nameToSeqMapping.get(name);
		switch ( keySeq ) {
		case 3:
			 return new TypedObject(this.timestampordepth);
		case 1:
			 return new TypedObject(this.category);
		case 2:
			 return new TypedObject(this.readingtype);
		case 0:
			 return new TypedObject(this.welboreid);
		case 4:
			 return new TypedObject(this.readingvalue);

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
	case 2:
		 return this.readingtype;
	case 0:
		 return this.welboreid;
	case 4:
		 return this.readingvalue;

		}
		return 0;
	}

	@Override
	public final float getFloatField(final int fldSequence) {
		switch ( fldSequence ) {

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
	case 3:
		 return this.timestampordepth;

		}
		return 0;
	}

	@Override
	public final String getStringField(final int fldSequence) {
		switch ( fldSequence ) {
	case 1:
		 return this.category;

		}
		return null;
	}

	@Override
	public final Object getObjectField(final int fldSequence) {
		return null;
	}
	
	@Override
	public final KVRowI create() {
		return new Wellbore();
	}

	@Override
	public final KVRowI create(final KVDataSchema dataSchema) {
		return new Wellbore(dataSchema);
	}
	
	@Override
	public final KVRowI create(final KVDataSchema dataSchema, final Integer id, final String mergeId) {
		
		Wellbore clonedInstance = new Wellbore(dataSchema);
		clonedInstance.__id__ = id;
		clonedInstance.__mergeId__ = mergeId;
		return clonedInstance;
	}


	@Override
	public final int getDataType(final String name) {
		return 0;
	}
	
	@Override
	public final void setId(final Integer id) {
		this.__id__ = id;
	}

	@Override
	public final int getId() {
		return this.__id__;
	}

	@Override
	public void setmergeId(String mergeId) {
		this.__mergeId__ = mergeId;
	}

	@Override
	public String getmergeId() {
		return this.__mergeId__;
	}
	

	@Override
	public int getValueSeq(String fldName) {
		return this.dataSchema.nameToSeqMapping.get(fldName);
	}
	
		@Override
	public Object getValue(int fldSeq) {
		switch ( fldSeq ) {
		case 3:
			 return this.timestampordepth;
		case 1:
			 return this.category;
		case 2:
			 return this.readingtype;
		case 0:
			 return this.welboreid;
		case 4:
			 return this.readingvalue;

		}
		return null;
	}	

}
