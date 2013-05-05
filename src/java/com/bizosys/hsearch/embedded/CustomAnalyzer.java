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
package com.bizosys.hsearch.embedded;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.util.LineReaderUtil;

public class CustomAnalyzer extends Analyzer {
	
	Set<String> stopwords = null;
	Map<String, String> conceptWithPipeSeparatedSynonums = null;
	char separator = '|';
	
	List<String> tempList = new ArrayList<String>();
	public CustomAnalyzer(Set<String> stopwords, Map<String, String> conceptWithPipeSeparatedSynonum) {
		this.stopwords = stopwords;
		this.conceptWithPipeSeparatedSynonums = conceptWithPipeSeparatedSynonum;
	}

	@Override
	public TokenStream tokenStream(String field, Reader reader) {

		Tokenizer tokenizer = new StandardTokenizer(Version.LUCENE_36, reader);
		TokenStream ts = new LowerCaseFilter(Version.LUCENE_36, tokenizer);
		 SynonymMap smap = null;
		 try {
			 if ( null != this.conceptWithPipeSeparatedSynonums) {
				 SynonymMap.Builder sb = new SynonymMap.Builder(true); 
				 for (String concept : this.conceptWithPipeSeparatedSynonums.keySet()) {
					 tempList.clear();
					 LineReaderUtil.fastSplit(tempList, this.conceptWithPipeSeparatedSynonums.get(concept), '|');
					 for (String syn : tempList) {
						 sb.add(new CharsRef(syn), new CharsRef(concept), false); 
					 }
				}
				smap = sb.build(); 
				if ( null != smap) ts = new SynonymFilter(ts, smap, true);
			 }

			ts = new PorterStemFilter(ts);
			if ( null != stopwords) {
				ts = new StopFilter(Version.LUCENE_36, ts, stopwords );
			} 
			 
			 return ts;
		 
		 } catch (IOException ex) {
			 ex.printStackTrace(System.err);
			 throw new NullPointerException(ex.toString());
		 }
	}
}