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

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.util.HSearchConfig;

public class DatablockFactory {
	
	static DatabaseReader reader = null;
	public static DatabaseReader initializeStorage() throws IOException  {
		String storageClass = HSearchConfig.getInstance().getConfiguration().get("hsearch.index.storage.class", "com.bizosys.hsearch.storage.DataBlockHBase");
		
		IdSearchLog.l.info("Storage Type is :" + storageClass);
		
		if ( null == reader) {
			if ( storageClass.equalsIgnoreCase("com.bizosys.hsearch.storage.DataBlockHBase")) {
				reader = new DataBlockHBase();
			} else {
				try {
					Object storageObject = Class.forName(storageClass).newInstance();
					if ( storageObject instanceof DatabaseReader  ) {
						reader = (DatabaseReader ) storageObject; 				
					} else {
						IdSearchLog.l.fatal("Unknown Storage :" + storageObject);
						throw new InstantiationException("Unknown Analyzer :" + storageObject);
					}
				} catch (InstantiationException e) {
					IdSearchLog.l.fatal(e);
					throw new IOException(storageClass, e);
				} catch (IllegalAccessException e) {
					IdSearchLog.l.fatal(e);
					throw new IOException(storageClass, e);
				} catch (ClassNotFoundException e) {
					IdSearchLog.l.fatal(e);
					throw new IOException(storageClass, e);
				}
			}
			
			if ( null != reader ) return reader.get();
			else throw new IOException("Reader is null");
		}
		
		
		try {
			return (DatabaseReader) Class.forName(storageClass).newInstance();
		} catch (Exception ex) {
			throw new IOException("Storage not implemented [hbase|hdfs|cassandra|jdbc|customclass - " + storageClass);			
		}
	}

}
