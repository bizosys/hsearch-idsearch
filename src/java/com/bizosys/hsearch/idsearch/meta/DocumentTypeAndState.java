/*
* Copyright 2010 Bizosys Technologies Limited
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

package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.Storable;

public class DocumentTypeAndState {
	
	public byte state = 0;
	public int documentType = -1;
	public long createdOn = -1;
	public long modifiedOn = -1;

	public DocumentTypeAndState(byte state, int documentType, long createdOn, long modifiedOn) {
		this.state = state;
		this.documentType = documentType;
		this.createdOn = createdOn;
		this.modifiedOn = modifiedOn;
	}
	
	public byte[] toBytes() throws IOException{
		
		byte[] docMetaFilterB = new byte[25];
		docMetaFilterB[0] = state;
		
		System.arraycopy(Storable.putInt(documentType), 0, docMetaFilterB, 1, 4);
		System.arraycopy(Storable.putLong(createdOn), 0, docMetaFilterB, 5, 8);
		System.arraycopy(Storable.putLong(modifiedOn), 0, docMetaFilterB, 13, 8);
		return docMetaFilterB;
	}
	
	
	public static DocumentTypeAndState build(byte[] data) throws IOException {
		return new DocumentTypeAndState( 
			data[0],
			Storable.getInt(1, data),
			Storable.getLong(5, data),
			Storable.getLong(13, data)
		);
	}
	
	public String toString() {
		return this.state + ":" + this.documentType + ":" + this.createdOn + ":" + this.modifiedOn;
	}
	
	public static void main(String[] args) throws IOException {
		DocumentTypeAndState serWeight = new DocumentTypeAndState((byte)1,23,55555555555555L,9666666666669L);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		DocumentTypeAndState dm = null;
		for ( int i=0; i<1000000; i++) {
			dm = DocumentTypeAndState.build(ser);
		}
		long end = System.currentTimeMillis();
		System.out.println ( dm.toString() + "   in " + (end - start) );
	}
}
