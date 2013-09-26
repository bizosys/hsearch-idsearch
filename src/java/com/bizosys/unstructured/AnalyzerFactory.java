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

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.bizosys.unstructured.util.Constants;
import com.bizosys.unstructured.util.IdSearchLog;
import com.sun.tools.corba.se.idl.InvalidArgument;

public class AnalyzerFactory {
	
	private static AnalyzerFactory singleton = null;
	
	public static AnalyzerFactory getInstance() {
		if ( null != singleton) return singleton;
		
		synchronized (AnalyzerFactory.class.getName()) {
			if ( null != singleton) return singleton;
			singleton = new AnalyzerFactory();
		}
		 return singleton;
	}
	
	Map<String, Analyzer> analyzerTypes = new HashMap<String, Analyzer>();
	private Analyzer defaultAnalyzer = null;

	private AnalyzerFactory() {
		this.defaultAnalyzer =  new StandardAnalyzer(Constants.LUCENE_VERSION);
	}

	public AnalyzerFactory setDefault(Analyzer analyzer) throws InvalidArgument{
		if ( null == analyzer) throw new InvalidArgument("Default Analyzer can not be null.");
		this.defaultAnalyzer = analyzer;
		return this;
	}
	
	public Analyzer getDefault() {
		return this.defaultAnalyzer;
	}
	
	public AnalyzerFactory creareBlankFactory() {
		return new AnalyzerFactory();
	}

	public Analyzer getAnalyzer(String docType, String fieldType) throws InstantiationException{
		if ( null == fieldType) fieldType = "";
		Analyzer analyzer = analyzerTypes.get(docType + "\t" + fieldType);
		if ( null == analyzer) analyzer = this.defaultAnalyzer;
		if ( null == analyzer) 
			throw new InstantiationException(
			"Analyzer not found for docType" + "\t" + fieldType);
		return analyzer;
	}

	public void setAnalyzer(String docType, String fieldType, Analyzer analyzer) {
		if ( null == fieldType) fieldType = "";
		analyzerTypes.put(docType + "\t" + fieldType, analyzer);
	}
	
	public void close() {
		for (Analyzer analyzer : analyzerTypes.values()) {
			if ( null != analyzer ) {
				try { analyzer.close(); 
				} catch (Exception ex) {
					IdSearchLog.l.warn("analyzerTypes closing:" + ex.getMessage());
				}
			}
		}
		analyzerTypes.clear();
		
		if ( null != this.defaultAnalyzer) {
			try { this.defaultAnalyzer.close(); 
			} catch (Exception ex) {
				IdSearchLog.l.warn("defaultAnalyzer closing:" + ex.getMessage());
			}
			this.defaultAnalyzer = null;
		}
	}
	
	public void close(Analyzer analyzer) {
		if ( null != analyzer ) {
			try { analyzer.close(); 
			} catch (Exception ex) {
				IdSearchLog.l.warn("AnalyzerFactory closing:" + ex.getMessage());
			}
		}

	}	

}
