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
package com.bizosys.hsearch.index;

import java.util.List;

import org.apache.lucene.analysis.TokenStream;

import com.bizosys.hsearch.index.util.StemFilterWrap;
import com.bizosys.hsearch.index.util.TermStream;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;


/**
 * Stem the terms
 * @author karan
 *
 */
public class FilterStem implements PipeIn {

	@Override
	public PipeIn getInstance() {
		return this;
	}

	@Override
	public String getName() {
		return "FilterLowercase";
	}

	@Override
	public void init(Configuration conf) {
	}

	@Override
	public void visit(PipeInData data) throws ApplicationFault, SystemFault {

		List<TermStream> streams = data.processingDocTokenStreams;
		if ( null == streams) {
			System.out.println("**************** No Stream");
			return; //Allow for no bodies
		}
		
		for (TermStream ts : streams) {
			TokenStream stream = ts.stream;
			if ( null == stream) {
				System.out.println("**************** No Stream");
				continue;
			}
			stream = new StemFilterWrap(stream);
			ts.stream = stream;
		}
	}
	
	@Override
	public void commit(PipeInData data) {
	}

	
}
