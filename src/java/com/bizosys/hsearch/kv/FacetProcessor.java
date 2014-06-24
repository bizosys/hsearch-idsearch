package com.bizosys.hsearch.kv;

import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.dao.MapperKVBaseEmptyImpl;

public class FacetProcessor extends MapperKVBaseEmptyImpl {
	
	public Map<Object, FacetCount> currentFacets = null; 
	public BitSetWrapper matchedIds = null;
	
	public FacetProcessor(BitSetWrapper matchedIds) {
		this.matchedIds = matchedIds;
	}
	
	public void setPreviousFacet(Map<Object, FacetCount> facetValues){
		if(null == facetValues)
			this.currentFacets = new HashMap<Object, FacetCount>(32);
		else
			this.currentFacets = facetValues;
	}
	
	@Override
	public boolean onRowCols(BitSetWrapper ids, Object value) {
		
		if ( null != matchedIds) 
			ids.and(matchedIds);
		
		int idsT = ids.cardinality();
		if(0 == idsT)
			return false;
		
		if ( currentFacets.containsKey(value)) {
			currentFacets.get(value).add(idsT);
		} else {
			currentFacets.put(value, new FacetCount(idsT));
		}

		return false;
	}

	@Override
	public boolean onRowCols(int key, Object value) {
		if (null != matchedIds) {
			if ( ! matchedIds.get(key)) return false;
		}
		if ( currentFacets.containsKey(value)) {
			currentFacets.get(value).count++;
		} else {
			currentFacets.put(value, new FacetCount());
		}
		
		return false;
	}
}
