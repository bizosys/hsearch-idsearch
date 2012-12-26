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
package com.bizosys.hsearch.unstructured.tokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.WhitespaceAnalyzer;

import com.bizosys.hsearch.unstructured.util.ContentFieldReader;
import com.bizosys.hsearch.unstructured.util.LuceneConstants;
import com.bizosys.hsearch.unstructured.util.TermStream;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.Configuration;

public class TokenizeWhitespace implements PipeIn {

	public TokenizeWhitespace() {
	}
	
	public PipeIn getInstance() {
		return this;
	}

	public String getName() {
		return "TokenizeWhitespace";
	}

	public void init(Configuration conf) {
	}

	public void visit(PipeInData data) throws ApplicationFault, SystemFault {

		data.processingDocFieldReaders = TokenizeBase.getReaders(data.processingDoc.fields);
    	if (null == data.processingDocFieldReaders) return;
		
		try {
			
    		Analyzer analyzer = new WhitespaceAnalyzer(LuceneConstants.version);
	    	for (ContentFieldReader reader : data.processingDocFieldReaders ) {
	    		TokenStream stream = analyzer.tokenStream( new Integer(reader.fieldName).toString(), reader.reader);
	    		TermStream ts = new TermStream( reader.fieldName, stream, reader.weight); 
	    		data.processingDocTokenStreams.add(ts);
			}
	    	analyzer.close();
    	} catch (Exception ex) {
    		throw new SystemFault(ex);
    	}
	}

	@Override
	public void commit(PipeInData data) {
	}
}
