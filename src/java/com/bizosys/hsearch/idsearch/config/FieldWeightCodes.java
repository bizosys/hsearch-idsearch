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
import java.util.HashMap;
import java.util.Map;

public class FieldWeightCodes extends _WeightCodes {
	
	private static FieldWeightCodes instance = null;
	public static FieldWeightCodes getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws IOException {
		instance = new FieldWeightCodes(data);
	}
	
	public FieldWeightCodes(byte[] data) throws IOException {
		super(data);
	}
	
	public static void main(String[] args) throws Exception {
		Map<String, Byte> weights = new HashMap<String, Byte>();
		weights.put("title", (byte) 100);
		weights.put("subject", (byte) 90);
		weights.put("body", (byte) 25);
		
		FieldWeightCodes.instanciate( FieldWeightCodes.builder().add(weights).toBytes() );
		System.out.println ( FieldWeightCodes.getInstance().getCode("body") );
		
	}

}
