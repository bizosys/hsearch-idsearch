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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
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

import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.unstructured.util.IdSearchLog;

public class StopwordAndSynonumAnalyzer extends Analyzer {
	
	Set<String> stopwords = null;
	Map<String, String> conceptWithPipeSeparatedSynonums = null;
	char conceptWordSeparator = '|';
	
	List<String> tempList = new ArrayList<String>();
	
	
	public StopwordAndSynonumAnalyzer() throws IOException {
		load();
	}
	
	public StopwordAndSynonumAnalyzer(Set<String> stopwords, Map<String, String> conceptWithPipeSeparatedSynonum, char conceptWordSeparator) {
		this.stopwords = stopwords;
		this.conceptWithPipeSeparatedSynonums = conceptWithPipeSeparatedSynonum;
		this.conceptWordSeparator = conceptWordSeparator;
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
					 LineReaderUtil.fastSplit(tempList, this.conceptWithPipeSeparatedSynonums.get(concept),
							 this.conceptWordSeparator);
					 for (String syn : tempList) {
						 sb.add(new CharsRef(syn), new CharsRef(concept), false); 
					 }
				}
				smap = sb.build();
				if ( null != smap) ts = new SynonymFilter(ts, smap, true);
			 }

			//ts = new PorterStemFilter(ts);
			if ( null != stopwords) {
				ts = new StopFilter(Version.LUCENE_36, ts, stopwords );
			} 
			 
			 return ts;
		 
		 } catch (IOException ex) {
			 ex.printStackTrace(System.err);
			 throw new NullPointerException(ex.toString());
		 }
	}

	public void load() throws IOException {
		String stopFile = this.getClass().getClassLoader().getResource("stopwords.txt").getPath();
		String synFile = this.getClass().getClassLoader().getResource("synonums.txt").getPath();
		
		load(stopFile, synFile);
	}
	
	public void load(String stopwordFileName, String synonumFileName) throws IOException {
		File stopwordFile = new File(stopwordFileName);
		this.stopwords = new HashSet<String>();
		this.conceptWithPipeSeparatedSynonums = new HashMap<String, String>();
		
		{
			BufferedReader reader = null;
			InputStream stream = null;
			try {
				stream = new FileInputStream(stopwordFile); 
				reader = new BufferedReader ( new InputStreamReader (stream) );
				String line = null;
				while((line=reader.readLine())!=null) {
					if (line.length() == 0) continue;
					this.stopwords.add(line);
				}
			} 
			
			catch (Exception ex) 
			{
				throw new IOException(ex);
			} 
			finally 
			{
				try {if ( null != reader ) reader.close();
				} catch (Exception ex) {IdSearchLog.l.warn(ex);}
				try {if ( null != stream) stream.close();
				} catch (Exception ex) {IdSearchLog.l.warn(ex);}
			}
		}
		/*****************
		 * CONCEPT FILE READING
		 ******************/
		{
			File synonumFile = new File(synonumFileName);
			BufferedReader reader = null;
			InputStream stream = null;
			try {
				stream = new FileInputStream(synonumFile); 
				reader = new BufferedReader ( new InputStreamReader (stream) );
				String line = null;
				
				List<String> concepts = new ArrayList<String>();
				while((line=reader.readLine())!=null) {
					if (line.length() == 0) continue;
					concepts.clear();
					
					int index1 = line.indexOf(this.conceptWordSeparator);
					if (index1 >= 0) {
						String left = line.substring(0, index1);
						conceptWithPipeSeparatedSynonums.put(left, line.substring(index1+1));
					} else {
						throw new IOException("Invalid Concept file format[" + line + "]");
					}
				}
			} 
			catch (Exception ex) 
			{
				throw new IOException(ex);
			} 
			finally 
			{
				try {if ( null != reader ) reader.close();
				} catch (Exception ex) {IdSearchLog.l.warn(ex);}
				try {if ( null != stream) stream.close();
				} catch (Exception ex) {IdSearchLog.l.warn(ex);}
			}		
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		
		Document doc = new Document();
		doc.add(new Field("description", "bengalure is a good city", Field.Store.NO, Field.Index.ANALYZED));
		Analyzer analyzer = new StopwordAndSynonumAnalyzer();

		for (Fieldable field : doc.getFields() ) {
    		StringReader sr = new StringReader(field.stringValue());
    		TokenStream stream = analyzer.tokenStream(field.name(), sr);
    		CharTermAttribute  termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
    		while ( stream.incrementToken()) {
    			System.out.println( "Term:" + termA.toString() );
    		}
    		sr.close();
		}	
		
		analyzer.close();
		
	}
}