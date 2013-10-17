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
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.unstructured.util.IdSearchLog;


public class SynonumAnalyzer extends Analyzer {
	
	Set<String> stopwords = null;
	Map<String, String> conceptWithPipeSeparatedSynonums = null;
	char conceptWordSeparator = '|';
	
	List<String> tempList = new ArrayList<String>();
	
	public SynonumAnalyzer() {
	}
	
	public void init() throws IOException {
		File stopwordsFile = getFile("stopwords.txt");
		if ( ! stopwordsFile.exists() ) {
			IdSearchLog.l.debug("Stopword file is not found.");
			stopwordsFile = null;
		}
		File conceptFile = getFile("concepts.txt");
		if ( ! conceptFile.exists() ) {
			IdSearchLog.l.debug("Concept file is not found.");
			conceptFile = null;
		}
		
		load(stopwordsFile, conceptFile);	

	}
	
	public void init(Set<String> stopwords, Map<String, String> concepts) throws IOException {
		this.stopwords = stopwords;
		this.conceptWithPipeSeparatedSynonums = concepts;
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
	
    public static File getFile(String fileName) 
    {
		File aFile = new File(fileName);
		if (aFile.exists()) return aFile;
		
		aFile = new File("/" + fileName);
		if (aFile.exists()) return aFile;

		try {
			URL resource = SynonumAnalyzer.class.getClassLoader().getResource(fileName);
			if ( resource != null) aFile = new File(resource.toURI());
		} 
		catch (URISyntaxException ex) {
			throw new RuntimeException(ex);
		}

		if (aFile.exists()) return aFile;

		throw new RuntimeException("SynonumAnalyzer > File does not exist :" + fileName);
	}	
    
	public void load(File stopwordFile, File synonumFile) throws IOException {
		
		this.stopwords = new HashSet<String>();
		this.conceptWithPipeSeparatedSynonums = new HashMap<String, String>();
		
		/*****************
		 * STOPWORD FILE READING
		 ******************/		
		if ( null != stopwordFile) {
			
			BufferedReader reader = null;
			InputStream stream = null;
			try {
				stream = new FileInputStream(synonumFile); 
				reader = new BufferedReader ( new InputStreamReader (stream) );
				String line = null;
				while((line=reader.readLine())!=null) {
					if (line.length() == 0) continue;
					stopwords.add(line);
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
		if ( null != synonumFile) {
			
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
	
	public static void main(String[] args) throws Exception {
		Document doc = new Document();
		doc.add(new Field("description", "hadoop_search", Field.Store.NO, Field.Index.ANALYZED));
		SynonumAnalyzer analyzer = new SynonumAnalyzer();
		analyzer.init();

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