package com.bizosys.hsearch.kv;

import java.util.ArrayList;
import java.util.List;

public class AMillionVOMemoryTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		List<ExamResult> results = new ArrayList<ExamResult>();
		for ( int i=0; i<1000000; i++) {
			ExamResult result = new ExamResult();
			result.__id__ = i;
			result.age = 23;
			results.add(result);
		}
	}

}
