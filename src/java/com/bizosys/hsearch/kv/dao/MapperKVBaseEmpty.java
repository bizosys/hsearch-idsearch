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
import java.util.Collection;

import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.kv.dao.MapperKVBase.TablePartsCallback;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

public abstract class MapperKVBaseEmpty extends MapperKVBase implements TablePartsCallback{
	
	@Override
	public void setOutputType(HSearchProcessingInstruction outputTypeCode){}
	
	@Override
	public void onReadComplete() {}
	
	@Override
	public BitSetOrSet getUniqueMatchingDocumentIds() throws IOException {return null;}
	
	@Override
	public void getResultSingleQuery(Collection<byte[]> rows) throws IOException {}
	
	@Override
	public void getResultMultiQuery(BitSetOrSet matchedIds,Collection<byte[]> rows) throws IOException {}
	
	@Override
	public void clear() {}
	
	@Override
	public TablePartsCallback getPart() {
		return this;
	}
	
	@Override
	public abstract boolean onRowCols(int key, Object value);

	@Override
	public abstract boolean onRowKey(int id);

}
