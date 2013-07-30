package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HsearchFacet {
	
	private String field = null;
	private TypedObject value = null;
	
	private List<HsearchFacet> internalFacets = null;

	public HsearchFacet(final String field, final TypedObject value, final List<HsearchFacet> internalFacets) {
		this.field = field;
		this.value = value;
		this.internalFacets = internalFacets;
	}

	public final String getField() {
		return this.field;
	}

	public final TypedObject getValue() throws IOException {
		return this.value;
	}

	public final List<HsearchFacet> getinternalFacets() {
		return this.internalFacets;
	}

	public final HsearchFacet getChild(final String field, final TypedObject data) throws IOException {
        for (HsearchFacet child: internalFacets ) {
            if (child.getValue().equals(data)) {
                return child;
            }
        }
        return getChild(new HsearchFacet(field, data, new ArrayList<HsearchFacet>()));
    }

	private final HsearchFacet getChild(final HsearchFacet child) {
        internalFacets.add(child);
        return child;
    }
	
	@Override
	public String toString() {
		return this.value +" "+ (this.internalFacets.size() == 0 ? "" : internalFacets.toString());
	}
}
