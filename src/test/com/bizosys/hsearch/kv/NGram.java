package com.bizosys.hsearch.kv;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NGram {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Set<String> termsL = new HashSet<String>();
		termsL.add("red");
		termsL.add( "party");
		termsL.add( "gown");
		termsL.add("dress");
		termsL.add("girls");
		
		String[] terms = new String[termsL.size()] ;
		termsL.toArray(terms);
		/**
		 * "red party gown"
		 * "party gown dress"
		 * "red party"
		 * "party gown"
		 * "gown dress"
		 * "red"
		 * "party"
		 * "gown"
		 * "dress"
		 */
		int termsT = terms.length;
		List<String> words = new ArrayList<String>();
		StringBuilder sb = new StringBuilder(1024);
		for (int subSequence=2; subSequence> 0; subSequence--) {
			if ( subSequence <= 0 ) break;
			
			for ( int wordPosition=0; wordPosition<= termsT- subSequence; wordPosition++ ) {
				
				for (int pos=0; pos< subSequence; pos++) {
					if ( pos > 0) sb.append(' ');
					sb.append(terms[wordPosition+pos]);
				}
				words.add(sb.toString());
				sb.setLength(0);
			}
		}
		System.out.println(words.toString());
		
	}

}
