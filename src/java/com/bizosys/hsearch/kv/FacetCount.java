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

package com.bizosys.hsearch.kv;

/**
 * 
 * This keeps track of count for given facet value.
 *
 */
public class FacetCount {

	public int count = 1;
	
	public FacetCount(){
		
	}
	
	public FacetCount(int count) {
		this.count = count;  
	}
	
	public void add(int count){
		this.count = this.count + count;
	}
	
	@Override
	public String toString() {
		return "(" + count + ")";
	}
}
