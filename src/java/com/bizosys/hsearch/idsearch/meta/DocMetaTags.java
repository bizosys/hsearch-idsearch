package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesString;

public class DocMetaTags {
	
	Collection<String> tags = null;
	
	public DocMetaTags(Collection<String> tags) {
		this.tags = tags;
	}
	
	public byte[] toBytes() throws IOException{
		return SortedBytesString.getInstance().toBytes(tags);
	}
	
	public static DocMetaTags build(byte[] data) throws IOException {
		return new DocMetaTags(SortedBytesString.getInstance().parse(data).values() );
	}
	
	public static boolean exists(byte[] data, String tag) throws IOException {
		return ((SortedBytesString.getInstance().parse(data).getEqualToIndex(tag)) != -1);
	}

	public String toString() {
		return this.tags.toString();
	}
	
	public static void main(String[] args) throws IOException {
		Set<String> tags = new HashSet<String>();
		tags.add("Abinash");
		tags.add("Bizosys");
		tags.add("hadoop");
		tags.add("Architect");
		
		
		DocMetaTags serWeight = new DocMetaTags(tags);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		for ( int i=0; i<1000000; i++) {
			DocMetaTags.exists(ser, "Abinash");
		}
		long end = System.currentTimeMillis();
		System.out.println (DocMetaTags.exists(ser, "Abinash") + "   in " + (end - start) );
	}
}
