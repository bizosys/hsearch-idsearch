package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.federate.QueryPart;

public class SearcherPluginTest implements ISearcherPlugin {

	public Map<String, Map<Object, FacetCount>> facetResult = null;

	@Override
	public void onJoin(String mergeId, BitSetWrapper foundIds,
			Map<String, QueryPart> whereParts,
			Map<Integer, KVRowI> foundIdWithValueObjects) {
		System.out.println("@@@@@@@@@@:" + foundIds.size());
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
	public void beforeSelectOnSorted(String mergeId, BitSetWrapper foundIds) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterSelectOnSorted(String mergeId, BitSetWrapper foundIds, Set<KVRowI> resultset) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean beforeSort(String mergeId, String sortFields,
			Set<KVRowI> resultSet,
			Map<Integer, BitSetWrapper> internalRanks) {
		return false;
	}

	@Override
	public void afterSort(String mergeId, Set<KVRowI> resultSet) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void afterSelect(String mergeId, BitSetWrapper foundIds,
			Set<KVRowI> resultset) throws IOException {
		// TODO Auto-generated method stub
		
	}

}
