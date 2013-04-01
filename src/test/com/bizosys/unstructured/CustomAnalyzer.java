package com.bizosys.unstructured;

import java.io.IOException;
import java.io.Reader;
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
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.Version;

public class CustomAnalyzer extends Analyzer {

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
			 String base1 = "abinash"; 
			 String syn1 = "abinasha"; 
			 String base2 = "bangalor"; 
			 String syn2 = "bangaloru"; 

			 SynonymMap.Builder sb = new SynonymMap.Builder(true); 
			 sb.add(new CharsRef(base1), new CharsRef(syn1), true); 
			 sb.add(new CharsRef(base2), new CharsRef(syn2), true); 
			 smap = sb.build(); 
		 } catch (IOException ex) {
			 ex.printStackTrace(System.err);
		 }
		 
		 ts = new SynonymFilter(ts, smap, true);
		 
		 return ts;
	}
}