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
package com.bizosys.hsearch.storage;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.storage.donotmodify.HSearchTableMultiQueryProcessorImpl;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.client.HSearchTableMultiQueryExecutor;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.storage.HSearchGenericFilter;
import com.bizosys.hsearch.util.HSearchLog;

public class Filter extends HSearchGenericFilter {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	
	public Filter(){
	}
	public Filter(HSearchProcessingInstruction outputType, String query,Map<String, String> details) {
		super(outputType, query, details);
	}

	@Override
	public HSearchTableMultiQueryExecutor createExecutor() {
		return new HSearchTableMultiQueryExecutor(new HSearchTableMultiQueryProcessorImpl());
	}

	@Override
	public IHSearchPlugin createPlugIn(String type) throws IOException {
		if (DEBUG_ENABLED) {
			HSearchLog.l.debug(Thread.currentThread().getId()+ " > HBaseHSearchFilter : type > " + type);
		}

		if ( type.equals("Documents") ) {
			return new MapperDocuments();
		}


		throw new IOException("Unknown Column Type :" + type);
	}

	@Override
	public HSearchReducer getReducer() {
		return new Reducer();
	}
}
