package com.bizosys.hsearch.kv;

import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.QueryPart;

public class SearcherPluginTest implements ISearcherPlugin {

	public Map<String, Map<Object, FacetCount>> facetResult = null;

	@Override
	public void onJoin(String mergeId, BitSetWrapper foundIds,
			Map<String, QueryPart> whereParts,
			Map<Integer, KVRowI> foundIdWithValueObjects) {
		
	}

	@Override
	public void onFacets(String mergeId,
			Map<String, Map<Object, FacetCount>> facets) {
		
		this.facetResult = facets;

	}

	@Override
	public void beforeSelect(String mergeId, BitSetWrapper foundIds) {
		// TODO Auto-generated method stub

	}

	@Override
	public void afterSelect(String mergeId, BitSetWrapper foundIds) {
		// TODO Auto-generated method stub

	}

}
