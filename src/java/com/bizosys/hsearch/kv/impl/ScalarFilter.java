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
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVBoolean;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVByte;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVDouble;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVFloat;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVIndex;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVInteger;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVLong;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVShort;
import com.bizosys.hsearch.kv.dao.donotmodify.HSearchTableKVString;
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

		switch ( type) {
			case 0:
				return new HSearchTableKVBoolean();
			case 1:
				return new HSearchTableKVByte();
			case 2:
				return new HSearchTableKVShort();
			case 3:
				return new HSearchTableKVInteger();
			case 4:
				return new HSearchTableKVFloat();
			case 5:
				return new HSearchTableKVLong();				
			case 6:
				return new HSearchTableKVDouble();
			case 7:
				return new HSearchTableKVString();
			case 8:
				return new HSearchTableKVIndex();
			default:
				return null;
		}
		
	}
	
}
