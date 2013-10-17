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
package com.bizosys.unstructured;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.PorterStemFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;

public class CustomAnalyzerExample extends Analyzer {

	@Override
	public TokenStream tokenStream(String field, Reader reader)  {
		 Tokenizer tokenizer = new StandardTokenizer(Version.LUCENE_36, reader);
		 TokenStream ts = new LowerCaseFilter(Version.LUCENE_36, tokenizer);
		 ts = new PorterStemFilter(ts);

		 Set<String> stopwords = new HashSet<String>();
		 stopwords.add("a");
		 stopwords.add("in");
		 ts = new StopFilter(Version.LUCENE_36, ts, stopwords );

		 SynonymMap smap = null;
		 try {
			 SynonymMap.Builder sb = new SynonymMap.Builder(true); 

			 String base1 = "abinash"; 
			 String syn1 = "abinasha"; 
			 String syn11 = "abinashak"; 
			 sb.add(new CharsRef(base1), new CharsRef(syn1), true); 
			 sb.add(new CharsRef(base1), new CharsRef(syn11), true); 
			 
			 String base2 = "bangalor"; 
			 String syn2 = "bangaloru"; 
			 sb.add(new CharsRef(base2), new CharsRef(syn2), true); 

			 smap = sb.build(); 
		 
		 } catch (IOException ex) {
			 ex.printStackTrace(System.err);
		 }
		 
		 ts = new SynonymFilter(ts, smap, true);
		 
		 return ts;
	}
	
	public static void main(String[] args) throws Exception {
		Document doc = new Document();
		doc.add(new Field("description", "Abinash", Field.Store.NO, Field.Index.ANALYZED));
		Analyzer analyzer = new CustomAnalyzerExample();

		for (Fieldable field : doc.getFields() ) {
    		StringReader sr = new StringReader(field.stringValue());
    		TokenStream stream = analyzer.tokenStream(field.name(), sr);
    		CharTermAttribute termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
    		while ( stream.incrementToken()) {
    			System.out.println( termA.toString() );
    		}
    		sr.close();
		}
	}
}