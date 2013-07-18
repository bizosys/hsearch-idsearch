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
package com.bizosys.unstructured;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.unstructured.IIndexPositionsTable;

public class DummyPositionsTable implements IIndexPositionsTable {

	@Override
	public void get(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException, NumberFormatException {
	}

	@Override
	public void keySet(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException {
	}

	@Override
	public void keyValues(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException {
	}

	@Override
	public void values(byte[] arg0, HSearchQuery arg1, IHSearchPlugin arg2)
			throws IOException {
	}

	@Override
	public void put(Integer docId, Integer docType, Integer fieldType, Integer hashCode, byte[] positions) {
		try {
			System.out.println(docId + "\t" + docType + "\t" + fieldType + "\t" + hashCode + "\t" +  
					SortedBytesInteger.getInstance().parse(positions).values().toString());
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
		}
	}

	@Override
	public byte[] toBytes() {
		return null;
	}

	@Override
	public void clear() {
	}


}
