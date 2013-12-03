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
package com.bizosys.unstructured;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;

public class SynonumAnalyzerExample {

	public static void main(String[] args) throws Exception {
		Document doc = new Document();
		doc.add(new Field("description", "bengalure is a good city", Field.Store.NO, Field.Index.ANALYZED));
		Map<String, String> syn = new HashMap<String, String>();
		syn.put("bangalore", "bengalure|bangaluru");
		Analyzer analyzer = new StopwordAndSynonymAnalyzer();
		//analyzer.load(null, syn);

		for (Fieldable field : doc.getFields() ) {
    		StringReader sr = new StringReader(field.stringValue());
    		TokenStream stream = analyzer.tokenStream(field.name(), sr);
    		CharTermAttribute  termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
    		while ( stream.incrementToken()) {
    			System.out.println( "Term:" + termA.toString() );
    		}
    		sr.close();
		}
	}

}
