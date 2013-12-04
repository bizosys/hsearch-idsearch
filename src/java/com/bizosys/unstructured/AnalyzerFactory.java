package com.bizosys.unstructured;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.analysis.SimpleAnalyzer;
import org.apache.lucene.analysis.WhitespaceAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.idsearch.util.Constants;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;

public class AnalyzerFactory {
		
	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	private static AnalyzerFactory singleton = null;
	
	public static AnalyzerFactory getInstance() {
		if ( null != singleton) return singleton;
		
		synchronized (AnalyzerFactory.class.getName()) {
			if ( null != singleton) return singleton;
			singleton = new AnalyzerFactory();
		}
		 return singleton;
	}
	
	Map<String, Analyzer> fieldNameWithAnalyzerInstance = new HashMap<String, Analyzer>();
	
	public Analyzer defaultAnalyzer = null;
	
	private AnalyzerFactory() {
		defaultAnalyzer = new StandardAnalyzer(Constants.LUCENE_VERSION);
	}
	
	public Analyzer getDefaultAnalyzer() {
		return defaultAnalyzer;
	}
	
	public void setDefaultAnalyzer(Analyzer analyzer) {
		this.defaultAnalyzer = analyzer;
	}
	
	public void init(FieldMapping fm) throws InstantiationException {
		for (Field fld : fm.nameWithField.values()) {
			if ( fld.isAnalyzed) {
				String analyzerClass = fld.analyzer;
				int analyzerClassLen = ( null == analyzerClass) ? 0 : analyzerClass.trim().length();
				if ( analyzerClassLen == 0 ) continue;
				
				Analyzer analyzer = null;
				
				if ( StandardAnalyzer.class.getName().equals(analyzerClass)) {
					analyzer = new StandardAnalyzer(Version.LUCENE_36);
				} else if ( KeywordAnalyzer.class.getName().equals(analyzerClass)) {
					analyzer = new KeywordAnalyzer();
				} else if ( SimpleAnalyzer.class.getName().equals(analyzerClass)) {
					analyzer = new SimpleAnalyzer(Version.LUCENE_36);
				} else if ( WhitespaceAnalyzer.class.getName().equals(analyzerClass)) {
					analyzer = new WhitespaceAnalyzer(Version.LUCENE_36);
				} else {
					instantiateAnalyzer(fld, analyzerClass);
				}
				if ( null != analyzer) 
					fieldNameWithAnalyzerInstance.put(fld.name, analyzer);				
				
			}
		}
	}

	private void instantiateAnalyzer(Field fld, String analyzerClass)
			throws InstantiationException {
		try {
			Object analyzerObj = Class.forName(analyzerClass).newInstance();
			if ( analyzerObj instanceof Analyzer  ) {
				fieldNameWithAnalyzerInstance.put(fld.name, (Analyzer) analyzerObj);				
			} else {
				IdSearchLog.l.warn("Unknown Analyzer :" + analyzerClass);
				throw new InstantiationException("Unknown Analyzer :" + analyzerClass);
			}
		} catch (InstantiationException e) {
			IdSearchLog.l.warn(e);
			throw e;
		} catch (IllegalAccessException e) {
			IdSearchLog.l.warn(e);
			throw new InstantiationException(e.getMessage());
		} catch (ClassNotFoundException e) {
			IdSearchLog.l.warn(e);
			throw new InstantiationException(e.getMessage());
		}
	}

	public Analyzer getAnalyzer(String field) {
		
		if ( fieldNameWithAnalyzerInstance.containsKey(field)) {
			return fieldNameWithAnalyzerInstance.get(field);
		} else {
			if  ( DEBUG_ENABLED)
				IdSearchLog.l.debug("No Analyzer for field:" + field);
		}
		
		return getDefaultAnalyzer();
	}
	
	public void close() {
		for (Analyzer analyzer : fieldNameWithAnalyzerInstance.values()) {
			try {
				analyzer.close();
			} catch (Exception ex) {
				//Eat the exception
				IdSearchLog.l.info("Error while closing analuyzer :" + ex.getMessage());
			}
		}
	}
}
