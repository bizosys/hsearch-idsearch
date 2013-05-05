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
package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesString;

public class Stopwords {
	
	Set<String> stopwords = new HashSet<String>();
	public Stopwords (byte[] data) throws IOException  {
		
		ISortedByte<String> wordsA = SortedBytesString.getInstance();
		wordsA.parse(data).values(stopwords);
	}
	
	
	public boolean hasWord(String word) throws IOException {
		return this.stopwords.contains(word);
	}
	
	public static Stopwords.Builder builder() {
		return new Stopwords.Builder();
	}	
	
	public static class Builder {
		
		Set<String> stopwords = new HashSet<String>();
		public Builder() {
		}		
		
		public byte[] toBytes() throws IOException {
			return SortedBytesString.getInstance().toBytes(stopwords);
		}		
		
		public Builder add(Collection<String> codes) {
			stopwords.addAll(codes);
			return this;
		}
		
		public Builder add(String word) {
			stopwords.add(word);
			return this;
		}

	}

}
