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

import com.bizosys.hsearch.kv.MapperKV;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVBoolean;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVByte;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVDouble;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVFloat;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVIndex;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVLong;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVShort;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVString;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.treetable.storage.HSearchScalarFilter;
import com.bizosys.hsearch.util.HSearchLog;

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
		int type = inputMapperInstructions.getOutputType();
		
		System.out.println("\n\n\n\n\n\n\n\n************" + inputMapperInstructions.getProcessingHint() + "*********************\n\n\n\n\n\n\n");

		switch ( type) {
			case Datatype.BOOLEAN:
				return new HSearchTableKVBoolean();
			case Datatype.BYTE:
				return new HSearchTableKVByte();
			case Datatype.SHORT:
				return new HSearchTableKVShort();
			case Datatype.INTEGER:
				return new HSearchTableKVInteger();
			case Datatype.FLOAT:
				return new HSearchTableKVFloat();
			case Datatype.LONG:
				return new HSearchTableKVLong();				
			case Datatype.DOUBLE:
				return new HSearchTableKVDouble();
			case Datatype.STRING:
				return new HSearchTableKVString();
			case Datatype.FREQUENCY_INDEX:
				return new HSearchTableKVIndex();
			default:
				return null;
		}
		
	}
	
}
