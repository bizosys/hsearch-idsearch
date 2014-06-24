package com.bizosys.unstructured;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.util.LuceneUtil;

public class FreeTextIndexingTest {

	private static final Pattern ASCII_PATTERN = Pattern.compile("[^\\p{ASCII}]+");
	
	public static void main(String[] args) throws ParseException, IOException,
	ClassNotFoundException, InstantiationException,
	IllegalAccessException, InterruptedException,
	org.apache.lucene.queryParser.ParseException {

		String indexData = "a o p v-neckline +dress dress/shirt-dress  www.bottica.com. price is [23.9876] available in {U.S.A,India} dress:shirt is good^5";
		//indexData = LuceneUtil.escapeLuceneSpecialCharacters(indexData);
		System.out.println(indexData);
		StopwordAndSynonymAnalyzer analyzer = new StopwordAndSynonymAnalyzer();
		TokenStream tsk = analyzer.tokenStream("K", new StringReader(indexData));
		CharTermAttribute term1 = tsk.getAttribute(CharTermAttribute.class);
		System.out.println();
		while(tsk.incrementToken()){
			System.out.println(term1.toString());
		}
		System.out.println("-------------------------------");
		TokenStream ts = new HSearchTokenizer(Version.LUCENE_36, new StringReader(indexData));
		CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
		while(ts.incrementToken()){
			System.out.println(term.toString());
		}
		System.out.println("-------------------------------");
		StandardAnalyzer stdAnalyzer = new StandardAnalyzer(Version.LUCENE_36);
		TokenStream stdTsk = stdAnalyzer.tokenStream("K", new StringReader(indexData));
		CharTermAttribute term2 = stdTsk.getAttribute(CharTermAttribute.class);
		System.out.println();
		while(stdTsk.incrementToken()){
			System.out.println(term2.toString());
		}
		
	}

	private static String rowKeyP1 = null;
	private static StringBuilder appender = new StringBuilder();;
	static Set<String> indexedWords = new LinkedHashSet<String>();

	public static final void mapFreeTextBitset(String fldValue)
			throws IOException, InterruptedException {

		CharTermAttribute termAttribute = null;
		TokenStream stream = null;
		Analyzer analyzer = null;
		try {
			StringReader sr = new StringReader(fldValue);
			analyzer = new StopwordAndSynonymAnalyzer();
			stream = analyzer.tokenStream("K", sr);
			termAttribute = (CharTermAttribute) stream
					.getAttribute(CharTermAttribute.class);
			String last2 = null;
			String last1 = null;

			while (stream.incrementToken()) {
				String termWord = termAttribute.toString();

				if (0 == termWord.length())
					continue;

				appender.delete(0, appender.capacity());
				rowKeyP1 = termWord;
				indexedWords.add(rowKeyP1);

				appender.setLength(0);
				if (null != last2) {
					appender.setLength(0);

					rowKeyP1 = appender.append(last2).append(' ').append(last1)
							.append(' ').append(termWord).toString();

					appender.setLength(0);
					indexedWords.add(rowKeyP1);

				}

				if (null != last1) {

					appender.setLength(0);
					rowKeyP1 = appender.append(last1).append(' ')
							.append(termWord).toString();

					appender.setLength(0);
					indexedWords.add(rowKeyP1);
				}

				last2 = last1;
				last1 = termWord;

			}

			System.out.println("\n" + indexedWords);

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (null != stream)
					stream.close();
				analyzer.close();
			} catch (Exception ex) {
				IdSearchLog.l.warn("Error during Tokenizer Stream closure");
			}
		}
	}

	public static void search(String query) throws IOException,
	org.apache.lucene.queryParser.ParseException {
		Analyzer analyzer = new StopwordAndSynonymAnalyzer();
		QueryParser qp = new QueryParser(Version.LUCENE_36, "K", analyzer);

		Query q = null;
		query = LuceneUtil.escapeLuceneSpecialCharacters(query);
		q = qp.parse(query);
		System.out.println(query);
		Set<Term> terms = new LinkedHashSet<Term>();
		q.extractTerms(terms);
		System.out.println("Extracted terms " + terms);
		int termsT = terms.size();
		switch (termsT) {
		case 2: {
			Iterator<Term> itr = terms.iterator();
			String word1 = itr.next().text();
			String word2 = itr.next().text();
			String phrase = word1 + " " + word2;
			if (indexedWords.contains(phrase)) {
				System.out.println("Found Biword " + phrase);
			} else {
				System.out.println("Found Singleword " + word1 + "/" + word2);
			}
		}
		break;
		case 3: {

			/**
			 * All 3 words are consecutive
			 */
			Iterator<Term> itr = terms.iterator();
			String word1 = itr.next().text();
			String word2 = itr.next().text();
			String word3 = itr.next().text();
			String phrase = word1 + " " + word2 + " " + word3;
			if (indexedWords.contains(phrase)) {
				System.out.println("Found Triword " + phrase);
			} else {
				String biword1 = word1 + " " + word2;
				String biword2 = word2 + " " + word3;
				String biword3 = word1 + " " + word3;
				String[] phrases = new String[] { biword1, biword2, biword3 };

				for (String phrase1 : phrases) {
					if (indexedWords.contains(phrase1))
						System.out.println("Found Biword " + phrase1);
				}
			}
		}
		break;
		case 4: {
			Iterator<Term> itr = terms.iterator();
			String word1 = itr.next().text();
			String word2 = itr.next().text();
			String word3 = itr.next().text();
			String word4 = itr.next().text();
			String triword1 = word1 + " " + word2 + " " + word3;
			String triword2 = word1 + " " + word3 + " " + word4;
			String triword3 = word2 + " " + word3 + " " + word4;
			String[] phrases = new String[] { triword1, triword2, triword3 };
			for (String phrase : phrases) {
				if (indexedWords.contains(phrase))
					System.out.println("Found Triword " + phrase);
			}

			String biword1 = word1 + " " + word2;
			String biword2 = word1 + " " + word3;
			String biword3 = word1 + " " + word4;
			String biword4 = word2 + " " + word3;
			String biword5 = word2 + " " + word4;

			String[] phrasesss = new String[] { biword1, biword2, biword3,
					biword4, biword5 };
			for (String phrase : phrasesss) {
				if (indexedWords.contains(phrase))
					System.out.println("Found Biword " + phrase);
			}
		}
		break;
		default:{
			String phrase = terms.iterator().next().text();
			if(indexedWords.contains(phrase))
				System.out.println("Found Singleword " + phrase);
		}
		}
	}


}
