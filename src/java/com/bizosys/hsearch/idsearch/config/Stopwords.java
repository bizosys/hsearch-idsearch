package com.bizosys.hsearch.idsearch.config;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.ByteArrays;

public class Stopwords {
	
	Set<String> stopwords = new HashSet<String>();
	public Stopwords (byte[] data) throws IOException  {
		
		ByteArrays.ArrayString wordsA = ByteArrays.ArrayString.parseFrom(data);
		stopwords.addAll(wordsA.getValList());
	}
	
	
	public boolean hasWord(String word) throws IOException {
		return this.stopwords.contains(word);
	}
	
	public static Stopwords.Builder builder() {
		return new Stopwords.Builder();
	}	
	
	public static class Builder {
		
		ByteArrays.ArrayString.Builder stopwords = null;
		public Builder() {
			stopwords = ByteArrays.ArrayString.newBuilder();
		}		
		
		public byte[] toBytes() throws IOException {
			return stopwords.build().toByteArray();
		}		
		
		public Builder add(Collection<String> codes) {
			for (String word : codes) {
				stopwords.addVal(word);
			}
			return this;
		}
		
		public Builder add(String word) {
			stopwords.addVal(word);
			return this;
		}

	}

}
