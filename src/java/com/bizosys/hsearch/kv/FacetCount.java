package com.bizosys.hsearch.kv;

public class FacetCount {

	public int count = 1;
	
	public FacetCount(){
		
	}
	
	public FacetCount(int count) {
		this.count = count;  
	}
	
	@Override
	public String toString() {
		return "(" + count + ")";
	}
}
