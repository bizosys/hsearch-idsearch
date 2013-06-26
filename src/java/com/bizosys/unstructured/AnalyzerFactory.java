package com.bizosys.unstructured;

import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.bizosys.unstructured.util.Constants;

public class AnalyzerFactory {
	
	Map<String, Analyzer> analyzerTypes = new HashMap<String, Analyzer>();
	Analyzer defaultAnalyzer = null;

	public AnalyzerFactory() {
	}
	
	public AnalyzerFactory (Analyzer analyzer)  {
		this.defaultAnalyzer = analyzer;
	}

	public void setDefault() {
		defaultAnalyzer = new StandardAnalyzer(Constants.LUCENE_VERSION);
	}
	
	public Analyzer getAnalyzer(String docType, String fieldType) throws InstantiationException{
		if ( null == fieldType) fieldType = "";
		Analyzer analyzer = analyzerTypes.get(docType + "\t" + fieldType);
		if ( null == analyzer) analyzer = defaultAnalyzer;
		if ( null == analyzer)  throw new InstantiationException(
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
				try { analyzer.close(); } catch (Exception ex) {/**Eat and digest*/}
			}
		}
		analyzerTypes.clear();
		
		if ( null != this.defaultAnalyzer) {
			System.out.println(this.defaultAnalyzer.toString());
			try { this.defaultAnalyzer.close(); } catch (Exception ex) {/**Eat and digest*/}
			this.defaultAnalyzer = null;
		}
	}

}
