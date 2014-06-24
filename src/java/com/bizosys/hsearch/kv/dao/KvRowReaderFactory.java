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

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.idsearch.util.ICache;
import com.bizosys.hsearch.idsearch.util.LruCache;
import com.bizosys.hsearch.storage.DatabaseReader;
import com.bizosys.hsearch.storage.DatabaseReaderCached;
import com.bizosys.hsearch.storage.DatablockFactory;

/**
 * 
 * A factory that returns the reader instances.
 *
 */
public class KvRowReaderFactory  {

	static KvRowReaderFactory instance = null;
	public static KvRowReaderFactory getInstance() throws IOException {
		if ( null != instance ) return instance;
		synchronized (KvRowReaderFactory.class.getName()) {
			if ( null != instance ) return instance;
			instance = new KvRowReaderFactory();
		}
		return instance;
	}
	
	private DatabaseReader reader = null;
	private DatabaseReader cachedReader = null;
	
	private KvRowReaderFactory() throws IOException {
		this.reader = DatablockFactory.initializeStorage();
		this.cachedReader = new DatabaseReaderCached(this.reader, true);
	}

	public BitSetWrapper getPinnedBitSets (String tableName, String rowId) throws IOException {

		BitSetWrapper dataBits = null;
			
		ICache cache = LruCache.getInstance();
		if ( cache.containsKey(rowId)) {
			dataBits = (BitSetWrapper) cache.getPinned(rowId);
		} else {
			
			byte[] dataA = reader.readRowBlob(tableName, rowId.getBytes());
			dataBits = SortedBytesBitset.getInstanceBitset().bytesToBitSet(dataA, 0, dataA.length);
			cache.putPinned(rowId, dataBits);
		}
		return dataBits;
	}
	
	/**
	 * 
	 * @param mergeId
	 * @return list of delete ids that need to be removed from saerch result.
	 */
	public String getDeleteId ( String mergeId) {
		return mergeId + "[[[deletes]]]";
	}
	
	/**
	 * 
	 * @param isCache
	 * @return reader instance use for reading hsearch index.
	 */
	public DatabaseReader getReader(boolean isCache) {
		
		if ( isCache) return this.cachedReader;
		else return this.reader;
	}	
}
