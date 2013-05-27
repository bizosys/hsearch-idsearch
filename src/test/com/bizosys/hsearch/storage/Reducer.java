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
