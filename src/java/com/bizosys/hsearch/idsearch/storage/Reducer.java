package com.bizosys.hsearch.idsearch.storage;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.functions.HSearchReducer;

public class Reducer implements HSearchReducer {
	
    @Override
    public void appendCols(Collection<byte[]> mergedL, Collection<byte[]> appendL) throws IOException {
        if (null == appendL) return;
        
        if (appendL.size() == 0) return;
        if (mergedL.size() == 0) {
            mergedL.addAll(appendL);
            return;
        }
        byte[] merged = mergedL.iterator().next();
        byte[] append = appendL.iterator().next();
        
        SortedBytesArray sa = SortedBytesArray.getInstanceArr();
        sa.parse(merged);
        byte[] keysMergedBytes = sa.getValueAt(0);
        byte[] valuesMergedBytes = sa.getValueAt(1);
        
        sa.parse(append);
        byte[] keysAppendBytes = sa.getValueAt(0);
        byte[] valuesAppendBytes = sa.getValueAt(1);        
        
        ISortedByte<String> keysMergedSorter = SortedBytesString.getInstance();
        keysMergedSorter.parse(keysMergedBytes);
        ISortedByte<Float> valuesMergedSorter = SortedBytesFloat.getInstance();
        valuesMergedSorter.parse(valuesMergedBytes);

        ISortedByte<String> keysAppendSorter = SortedBytesString.getInstance();
        keysAppendSorter.parse(keysAppendBytes);
        ISortedByte<Float> valuesAppendSorter = SortedBytesFloat.getInstance();
        valuesAppendSorter.parse(valuesAppendBytes);
        
        int sizeM = keysMergedSorter.getSize();
        Map<String, Float> joinedRows = new HashMap<String, Float>();
        for ( int i=0; i<sizeM; i++) {
        	joinedRows.put(keysMergedSorter.getValueAt(i), valuesMergedSorter.getValueAt(i));
        }
        
        int sizeA = keysAppendSorter.getSize();
        for ( int i=0; i<sizeA; i++) {
        	String appendKey = keysAppendSorter.getValueAt(i);
        	if ( joinedRows.containsKey(appendKey)) {
        		float totalWeight = 10 * 
        			(joinedRows.get(appendKey).floatValue() + valuesAppendSorter.getValueAt(i).floatValue());
        		joinedRows.put(appendKey, totalWeight);
        	}
        }
        mergedL.clear();
        
        MapperDocuments.ser(mergedL, joinedRows);        

    }
    
    @Override
    public void appendRows(Collection<byte[]> mergedB, Collection<byte[]> appendB) {
    	mergedB.addAll(appendB);
    }
    @Override
    public void appendRows(Collection<byte[]> mergedRows, byte[] appendRowId, Collection<byte[]> appendRows) {
    	mergedRows.addAll(appendRows);
    }
}
