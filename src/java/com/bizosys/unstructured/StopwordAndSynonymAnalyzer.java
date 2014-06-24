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
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.analysis.ASCIIFoldingFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.synonym.SynonymFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;
import org.tartarus.snowball.ext.EnglishStemmer;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.util.HSearchConfig;
import com.bizosys.hsearch.util.LineReaderUtil;
import com.bizosys.hsearch.util.conf.Configuration;

public class StopwordAndSynonymAnalyzer extends Analyzer {
	
	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();
	static Set<String> stopwords = null;
	static Map<String, String> conceptWithPipeSeparatedSynonums = null;
	char conceptWordSeparator = '|';

	
	boolean isLowerCaseEnabled = true;
	boolean isAccentFilterEnabled = true;
	boolean isSnoballStemEnabled = true;
	boolean isStopFilterEnabled = true;
	
	public StopwordAndSynonymAnalyzer() throws IOException {
		load();
	}
	
	public StopwordAndSynonymAnalyzer(Set<String> stopwords, Map<String, String> conceptWithPipeSeparatedSynonum, char conceptWordSeparator) {
		StopwordAndSynonymAnalyzer.stopwords = stopwords;
		conceptWithPipeSeparatedSynonums = conceptWithPipeSeparatedSynonum;
		this.conceptWordSeparator = conceptWordSeparator;
	}

	@Override
	public TokenStream tokenStream(String field, Reader reader) {
		
		TokenStream ts = new HSearchTokenizer(Version.LUCENE_36, reader);
		ts = new LowerCaseFilter(Version.LUCENE_36, ts);
		
		SynonymMap smap = null;
		 try {
			 if ( null != conceptWithPipeSeparatedSynonums) {
				 SynonymMap.Builder sb = new SynonymMap.Builder(true);
				 List<String> tempList = new ArrayList<String>();

				 for (String concept : conceptWithPipeSeparatedSynonums.keySet()) {
					 tempList.clear();
					 LineReaderUtil.fastSplit(tempList, conceptWithPipeSeparatedSynonums.get(concept),
							 this.conceptWordSeparator);
					 for (String syn : tempList) {
						 int synLen = ( null == syn ) ? 0 : syn.length();
						 if ( synLen == 0 ) continue;
						 sb.add(new CharsRef(syn), new CharsRef(concept), false); 
					 }
				}
				if (conceptWithPipeSeparatedSynonums.size() > 0 ) {
					smap = sb.build();
					if ( null != smap) ts = new SynonymFilter(ts, smap, true);
				}
			 }
			
			if(isStopFilterEnabled){
				int stopwordsT = ( null == stopwords) ? 0 : stopwords.size();
				if ( stopwordsT > 0 ) {
					ts = new StopFilter(Version.LUCENE_36, ts, stopwords );
				} 				
			}

			if(isAccentFilterEnabled)
				ts = new ASCIIFoldingFilter(ts);
			if(isSnoballStemEnabled)
				ts = new SnowballFilter(ts, new EnglishStemmer());

			return ts;
		 
		 } catch (IOException ex) {
			 ex.printStackTrace(System.err);
			 throw new NullPointerException(ex.toString());
		 }
	}

	public void load() throws IOException {
		
		InputStream stopwordStream = null;
		InputStream synonumStream = null;
		
		Configuration hsearchConf = HSearchConfig.getInstance().getConfiguration();
		String filenameSynonum = hsearchConf.get("synonyms.file.location", "synonyms.txt");
		String filenameStopword = hsearchConf.get("stopword.file.location", "stopwords.txt");
		
		isLowerCaseEnabled = hsearchConf.getBoolean("lucene.analysis.lowercasefilter", true);
		isAccentFilterEnabled = hsearchConf.getBoolean("lucene.analysis.accentfilter", true);
		isSnoballStemEnabled = hsearchConf.getBoolean("lucene.analysis.snowballfilter", true);
		isStopFilterEnabled = hsearchConf.getBoolean("lucene.analysis.stopfilter", true);
		
		if ( null != stopwords) return;
		
		org.apache.hadoop.conf.Configuration conf = 
			new org.apache.hadoop.conf.Configuration();
		FileSystem fs = FileSystem.get(conf);

		if ( null != fs) {
			
			/**
			 * STOPWORD
			 */
			Path stopPath = new Path(filenameStopword);
			if ( fs.exists(stopPath) ) {
				if ( DEBUG_ENABLED) IdSearchLog.l.debug( "Loading Stopword file from HDFS :" + stopPath.toString());
				stopwordStream = fs.open(stopPath);
			} else {
				IdSearchLog.l.fatal("Stopword file not available in HDFS :" + stopPath.toString());
			}
			
			/**
			 * SYNONUM
			 */
			
			Path synPath = new Path(filenameSynonum);
			if ( fs.exists(synPath) ) {
				synonumStream = fs.open(synPath);
				if ( DEBUG_ENABLED) IdSearchLog.l.debug("Loading synonym file from HDFS :" + filenameSynonum.toString());
			} else {
				IdSearchLog.l.fatal("Synonym file not available in HDFS :" + filenameSynonum.toString());
				IdSearchLog.l.fatal("Working Directory :" + fs.getWorkingDirectory().getName() );
			}
		}
		
		ClassLoader classLoader = null;
		
		if ( null == stopwordStream || null == synonumStream) {
			classLoader =Thread.currentThread().getContextClassLoader();
		}
		
		if ( null == stopwordStream) {
			URL stopUrl = classLoader.getResource(filenameStopword);
			if ( null != stopUrl) {
				String stopFile = stopUrl.getPath();
				if ( null != stopFile) {
					File stopwordFile = new File(stopFile);
					if ( stopwordFile.exists() && stopwordFile.canRead()) {
						stopwordStream = new FileInputStream(stopwordFile);
						if ( DEBUG_ENABLED) IdSearchLog.l.debug("Loading Stopword file from Local :" + stopwordFile.getAbsolutePath());
					} else {
						IdSearchLog.l.fatal("Stopword file not available at :" + stopwordFile.getAbsolutePath());
						IdSearchLog.l.fatal("Working Directory :" + fs.getHomeDirectory().getName() );
					}
				} else {
					if ( DEBUG_ENABLED) IdSearchLog.l.debug("Ignoring Stopwords > " + filenameStopword);
				}
			}
		}

		if ( null == synonumStream) {
			URL synUrl = classLoader.getResource(filenameSynonum);
			if ( null != synUrl) {
				String synFileName = synUrl.getPath();
				if ( null != synFileName) {
					File synFile = new File(synFileName);
					if ( synFile.exists() && synFile.canRead()) {
						synonumStream = new FileInputStream(synFile);
						if ( DEBUG_ENABLED) IdSearchLog.l.debug("Loading Synonum file from Local :" + synFile.getAbsolutePath());
					} else {
						if ( DEBUG_ENABLED) IdSearchLog.l.debug("Synonum file not available at :" + synFile.getAbsolutePath());
					}
				} else {
					if ( DEBUG_ENABLED) IdSearchLog.l.debug("Ignoring Synonyms > " + filenameSynonum);
				}
			}
		}
		
		load(stopwordStream, synonumStream);
	}
	
	public void load(InputStreamReader stopwordStream, InputStreamReader synonumStream) throws IOException {
		if ( null != stopwords) return;
		
		stopwords = new HashSet<String>();
		conceptWithPipeSeparatedSynonums = new HashMap<String, String>();
		
		{
			BufferedReader reader = null;
			try {
				reader = new BufferedReader ( stopwordStream );
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
				try {if ( null != stopwordStream) stopwordStream.close();
				} catch (Exception ex) {IdSearchLog.l.warn(ex);}
			}
		}
		/*****************
		 * CONCEPT FILE READING
		 ******************/
		{
			BufferedReader reader = null;
			try {
				reader = new BufferedReader ( synonumStream );
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
				try {if ( null != synonumStream) synonumStream.close();
				} catch (Exception ex) {IdSearchLog.l.warn(ex);}
			}		
		}
		
	}	
	
	public void load(InputStream stopwordStream, InputStream synonumStream) throws IOException {
		stopwords = new HashSet<String>();
		conceptWithPipeSeparatedSynonums = new HashMap<String, String>();
		
		if ( null != stopwordStream) loadStopwords(stopwordStream);
		if ( null != synonumStream) loadSynonums(synonumStream);
	}

	private void loadSynonums(InputStream synonumStream) throws IOException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader ( new InputStreamReader (synonumStream) );
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
			try {if ( null != synonumStream) synonumStream.close();
			} catch (Exception ex) {IdSearchLog.l.warn(ex);}
		}
	}

	private void loadStopwords(InputStream stopwordStream) throws IOException {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader ( new InputStreamReader (stopwordStream) );
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
			try {if ( null != stopwordStream) stopwordStream.close();
			} catch (Exception ex) {IdSearchLog.l.warn(ex);}
		}
	}
	
	public static void main(String[] args) throws IOException {
		
		Document doc = new Document();
		doc.add(new Field("description", "dress/t-shirt dress for \"good boy\"", Field.Store.NO, Field.Index.ANALYZED));
		Analyzer analyzer = new StopwordAndSynonymAnalyzer();
		
		for (Fieldable field : doc.getFields() ) {
			String query = "dress/t-shirt dress for \"good boy\"";
    		StringReader sr = new StringReader(query);
    		TokenStream stream = analyzer.tokenStream(field.name(), sr);
    		CharTermAttribute  termA = (CharTermAttribute)stream.getAttribute(CharTermAttribute.class);
    		
    		if ( DEBUG_ENABLED ) {
        		while ( stream.incrementToken()) {
        			IdSearchLog.l.debug( "Term:" + termA.toString() );
        		}
    		}
    		sr.close();
		}	
		
		analyzer.close();
		
	}
}