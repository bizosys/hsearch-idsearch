package com.bizosys.hsearch.kv;

import java.util.Map;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.QueryPart;

public class SearcherPluginBase implements ISearcherPlugin {

	
	public void onJoin(String mergeId, BitSetWrapper foundIds,
			Map<String, QueryPart> whereParts,
			Map<Integer, KVRowI> foundIdWithValueObjects) {
	}

	@Override
	public void onFacets(String mergeId,
			Map<String, Map<Object, FacetCount>> facets) {
	}

	@Override
	public void beforeSelect(String mergeId, BitSetWrapper foundIds) {
	}

	@Override
	public void afterSelect(String mergeId, BitSetWrapper foundIds) {
	}

}
