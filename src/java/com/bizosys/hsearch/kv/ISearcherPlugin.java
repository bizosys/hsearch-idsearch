package com.bizosys.hsearch.kv;

import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.QueryPart;

public interface ISearcherPlugin {
	
	void onJoin(String mergeId, BitSetWrapper foundIds, 
		Map<String, QueryPart> whereParts, Map<Integer,KVRowI> foundIdWithValueObjects);
	
	void onFacets(String mergeId, Map<String, Map<Object, FacetCount>> facets);

	void beforeSelect(String mergeId, BitSetWrapper foundIds);
	
	void afterSelect(String mergeId, BitSetWrapper foundIds);

}
