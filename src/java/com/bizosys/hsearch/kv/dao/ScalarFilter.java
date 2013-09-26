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
package com.bizosys.hsearch.kv.dao;

import java.io.IOException;

import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVBooleanInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVByteInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVDoubleInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVFloatInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVIntegerInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVLongInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVShortInverted;
import com.bizosys.hsearch.kv.dao.inverted.HSearchTableKVStringInverted;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVBoolean;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVByte;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVDouble;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVFloat;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVIndex;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVLong;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVShort;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVString;
import com.bizosys.hsearch.kv.impl.Datatype;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.treetable.storage.HSearchScalarFilter;
import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.unstructured.util.IdSearchLog;

public class ScalarFilter extends HSearchScalarFilter {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	public ScalarFilter(){
	}
	
	public ScalarFilter(HSearchProcessingInstruction outputType, String query) {
		super(outputType, query);
	}
	

	@Override
	public IHSearchPlugin createPlugIn() throws IOException {
		return new MapperKV();
	}

	@Override
	public IHSearchTable createTable() {
		return createTable(inputMapperInstructions);
		
	}

	public static IHSearchTable createTable(HSearchProcessingInstruction inputMapperInstructions) {
		int type = inputMapperInstructions.getOutputType();
		String hint = inputMapperInstructions.getProcessingHint();
		boolean isRepeating = hint.startsWith("true");
		boolean isCompressed = hint.endsWith("true");
		
		switch ( type) {
			case Datatype.BOOLEAN:
				if ( isRepeating ) return new HSearchTableKVBooleanInverted(isCompressed);
				return new HSearchTableKVBoolean();
			case Datatype.BYTE:
				if ( isRepeating ) return new HSearchTableKVByteInverted(isCompressed);
				return new HSearchTableKVByte();
			case Datatype.SHORT:
				if ( isRepeating ) return new HSearchTableKVShortInverted(isCompressed);
				return new HSearchTableKVShort();
			case Datatype.INTEGER:
				if ( isRepeating ) return new HSearchTableKVIntegerInverted(isCompressed);
				return new HSearchTableKVInteger();
			case Datatype.FLOAT:
				if ( isRepeating ) return new HSearchTableKVFloatInverted(isCompressed);
				return new HSearchTableKVFloat();
			case Datatype.LONG:
				if ( isRepeating ) return new HSearchTableKVLongInverted(isCompressed);
				return new HSearchTableKVLong();				
			case Datatype.DOUBLE:
				if ( isRepeating ) return new HSearchTableKVDoubleInverted(isCompressed);
				return new HSearchTableKVDouble();
			case Datatype.STRING:
				if ( isRepeating ) return new HSearchTableKVStringInverted(isCompressed);
				return new HSearchTableKVString();
			case Datatype.FREQUENCY_INDEX:
				return new HSearchTableKVIndex();
			default:
				IdSearchLog.l.error("ScalarFilter: Unknown Type : " + type);
				return null;
		}
	}
	
}
