/*
* Copyright 2013 Bizosys Technologies Limited
*
* Licensed to the Bizosys Technologies Limited (Bizosys) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The Bizosys licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.kv.impl.TypedObject;

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
        HsearchFacet facet = new HsearchFacet(field, data, new ArrayList<HsearchFacet>()); 
        internalFacets.add(facet);
        return facet;
    }
	
	@Override
	public String toString() {
		return this.value +" "+ (this.internalFacets.size() == 0 ? "" : internalFacets.toString());
	}
}