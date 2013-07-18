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
package com.bizosys.hsearch.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.functions.HSearchReducer;
import com.bizosys.hsearch.functions.StatementWithOutput;

public class Reducer implements HSearchReducer {

	
    @Override
	public void appendCols(StatementWithOutput[] queryOutput, Collection<byte[]> finalColumns) throws IOException {
    }

    @Override
    public void appendRows(Collection<byte[]> mergedB, Collection<byte[]> appendB) {

        if (null == appendB) return;
        if (appendB.size() == 0) return;

        byte[] append = appendB.iterator().next();
        if (null == append) return;
        
        byte[] merged = null;
        if (mergedB.size() == 0) {
            merged = new byte[append.length];
            Arrays.fill(merged, (byte) 0);
        } else {
            merged = mergedB.iterator().next();
        }

        /**
         * De-serialize and compute.
         */

        /**
         * Serialize at the end
         */

        mergedB.clear();

        /**
         * Add the processed bytes to merged container
         */
    }

    @Override
    public void appendRows(Collection<byte[]> mergedRows, byte[] appendRowId, Collection<byte[]> appendRows) {
        appendRows(mergedRows, appendRows);

    }

}
