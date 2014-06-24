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
package com.bizosys.hsearch.kv.impl.bytescooker;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;

import com.bizosys.hsearch.kv.indexing.KVIndexer;

public abstract class IndexField {
	
	public byte[] index(Iterable<Text> values) throws IOException {
		
		byte[] finalData = null;
		boolean hasValue = false;
    	String[] resultValue = new String[2];
    	String line = null;
    	String currentF = null;

    	try {
    		
			for (Text text : values) {
				if ( null == text) continue;
				Arrays.fill(resultValue, null);
	
				line = text.toString();
				
				int index = line.indexOf(KVIndexer.FIELD_SEPARATOR);
				if (index >= 0) {
					resultValue[0] = line.substring(0, index);
					if (index <= line.length() - 1) resultValue[1] = line.substring(index + 1);
				}
						
				currentF = resultValue[0];
				int containerKey = Integer.parseInt(currentF);
				hasValue = true;
				add(containerKey, resultValue[1]);
			}
    	} catch (NumberFormatException ex) {
    		ex.printStackTrace();
    		throw new IOException("Unable to parse number - [" + currentF + "] for input " + line + " with line sep :" + KVIndexer.FIELD_SEPARATOR + " because " + ex.getMessage());
    	}
	
		
		if ( hasValue ) {
			finalData = getBytes();
		}
		return finalData;
	}
	
	public abstract void add(int key, String val);
	public abstract void append(byte[] vals) throws IOException;
	public abstract byte[] getBytes() throws IOException;	
}
