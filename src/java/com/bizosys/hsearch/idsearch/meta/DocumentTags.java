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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesString;

public class DocumentTags {
	
	Collection<String> tags = null;
	
	public DocumentTags(Collection<String> tags) {
		this.tags = tags;
	}
	
	public byte[] toBytes() throws IOException{
		return SortedBytesString.getInstance().toBytes(tags);
	}
	
	public static DocumentTags build(byte[] data) throws IOException {
		return new DocumentTags(SortedBytesString.getInstance().parse(data).values() );
	}
	
	public static boolean exists(byte[] data, String tag) throws IOException {
		return ((SortedBytesString.getInstance().parse(data).getEqualToIndex(tag)) != -1);
	}

	public String toString() {
		return this.tags.toString();
	}
	
	public static void main(String[] args) throws IOException {
		Set<String> tags = new HashSet<String>();
		tags.add("Abinash");
		tags.add("Bizosys");
		tags.add("hadoop");
		tags.add("Architect");
		
		
		DocumentTags serWeight = new DocumentTags(tags);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		for ( int i=0; i<1000000; i++) {
			DocumentTags.exists(ser, "Abinash");
		}
		long end = System.currentTimeMillis();
		System.out.println (DocumentTags.exists(ser, "Abinash") + "   in " + (end - start) );
	}
}
