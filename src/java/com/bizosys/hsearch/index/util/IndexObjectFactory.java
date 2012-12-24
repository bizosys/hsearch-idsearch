package com.bizosys.hsearch.index.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class IndexObjectFactory {

	private static int MINIMUM_CACHE = 10;
	private static int MAXIMUM_CACHE = 4096;
	
	private static IndexObjectFactory thisInstance = new IndexObjectFactory();
	public static IndexObjectFactory getInstance() {
		return thisInstance;
	}
	
	
	Stack<List<TermStream>> streamLists = new Stack<List<TermStream>>();
	
	public  List<TermStream> getStreamList() {
		List<TermStream> streams = null;
		if (streamLists.size() > MINIMUM_CACHE ) streams = streamLists.pop();
		if ( null != streams ) return streams;
		return new ArrayList<TermStream>();
	}
}
