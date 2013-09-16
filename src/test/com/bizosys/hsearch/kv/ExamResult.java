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

import com.bizosys.hsearch.functions.GroupSortedObject;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.bizosys.hsearch.kv.TypedObject;

public class ExamResult extends GroupSortedObject implements KVRowI{
	
	public String mergeId = null;
	public int empid = 0;
	public String classz = null;
	public float marks = 0.0f;
	public String location = null;
	public int age = 0;
	public String role = null;
	public String comments = null;

		
	KVDataSchema dataSchema = null;
	
	public ExamResult() {}
	public ExamResult(KVDataSchema dataScheme) {
		this.dataSchema = dataScheme;
	}

	public final void setValue(final String key, final Object value){
		int keySeq = this.dataSchema.nameToSeqMapping.get(key);
		switch ( keySeq ) {
		case 1:
			 this.empid = (Integer)value;
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
		case 3:
			 this.role = value.toString();
		 break;
		case 7:
			 this.comments = value.toString();
		 break;

		}
	}
	
		@Override
	public final Object getValue(final String key){
		int keySeq = this.dataSchema.nameToSeqMapping.get(key);
		switch ( keySeq ) {
		case 1:
			 return this.empid;
		case 0:
			 return this.classz;
		case 5:
			 return this.marks;
		case 4:
			 return this.location;
		case 2:
			 return this.age;
		case 3:
			 return this.role;
		case 7:
			 return this.comments;

		}
		return null;
	}

	@Override
	public TypedObject getValueNative(String name) {
		int keySeq = this.dataSchema.nameToSeqMapping.get(name);
		switch ( keySeq ) {
		case 1:
			 return new TypedObject(this.empid);
		case 0:
			 return new TypedObject(this.classz);
		case 5:
			 return new TypedObject(this.marks);
		case 4:
			 return new TypedObject(this.location);
		case 2:
			 return new TypedObject(this.age);
		case 3:
			 return new TypedObject(this.role);
		case 7:
			 return new TypedObject(this.comments);

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
	case 0:
		 return this.classz;
	case 4:
		 return this.location;
	case 3:
		 return this.role;
	case 7:
		 return this.comments;

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
		this.empid = id;
	}

	@Override
	public final int getId() {
		return this.empid;
	}

	@Override
	public void setmergeId(String mergeId) {
		this.mergeId = mergeId;
	}

	@Override
	public String getmergeId() {
		return this.mergeId;
	}

}
