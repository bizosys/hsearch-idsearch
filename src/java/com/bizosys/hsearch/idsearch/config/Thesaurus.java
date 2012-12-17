package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.dictionary.Dictionary;

public class Thesaurus extends _MapDetails {
	
	private static Dictionary dict = Dictionary.getInstance();
	private static Thesaurus instance = null;

	public static Thesaurus getInstance() throws InstantiationException {
		if ( null == instance) throw new InstantiationException();
		return instance;
	}

	public static void instanciate(byte[] data) throws Exception {
		
		instance = new Thesaurus(data);
		
		StringWriter responseWriter = new StringWriter();
		dict.load(instance.nameLines, responseWriter, false);
	}
	
	public Thesaurus(byte[] data) throws IOException {
		super(data);
	}
	
	public String findTopWord(String query) throws Exception {
		if ( containsExact(query)) return query;
		return dict.findTopDocument(query, true);
	}
	
	public String findTopDescription(String query, boolean isWord) throws Exception {
		if ( containsExact(query)) {
			if ( isWord ) return query;
			else this.nameLines.get(query);
		}
		return dict.findTopDocument(query, isWord);
	}

	public List<String> predict(String query) throws Exception {
		return dict.predict(query);
	}
	
	public boolean containsExact(String query) throws Exception {
		return this.nameLines.containsKey(query);
	}
	
	public String findExact(String query) throws Exception {
		if ( this.nameLines.containsKey(query)) return this.nameLines.get(query);
		return "";
	}

	public static void main(String[] args) throws Exception {
		Map<String, String> words = new HashMap<String, String>();
		words.put("john", "fname");
		words.put("abinash", "fname");
		
		Thesaurus.instanciate( Thesaurus.builder().add(words).toBytes());
		System.out.println ( Thesaurus.getInstance().findTopWord("joh") );
	}	
	
}
