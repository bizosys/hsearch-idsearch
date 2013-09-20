package com.bizosys.hsearch.kv.dao.inverted;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.byteutils.SortedBytesDouble;
import com.bizosys.hsearch.kv.dao.MapperKVBase;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

public class HSearchTableKVDoubleInverted implements IHSearchTable {

    public static boolean DEBUG_ENABLED = false;
    
    public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;

    public static final class Cell2FilterVisitor implements Cell2Visitor<BitSet, Double> {

        public HSearchQuery query;
        public IHSearchPlugin plugin;
        public MapperKVBase.TablePartsCallback tablePartsCallback = null;

        public Integer matchingCell0;
        public Integer cellMin0;
        public Integer cellMax0;
        public Integer[] inValues0;

        public int cellFoundKey;

        public int mode = MODE_COLS;

        public Cell2FilterVisitor(HSearchQuery query,
                IHSearchPlugin plugin, MapperKVBase.TablePartsCallback tablePartsCallback, int mode) {

            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }

        public void set(Integer matchingCell2, Integer cellMin2, Integer cellMax2, Integer[] inValues2) {
            this.matchingCell0 = matchingCell2;
            this.cellMin0 = cellMin2;
            this.cellMax0 = cellMax2;
            this.inValues0 = inValues2;
        }

        @Override
        public final void visit(BitSet cell1Key, Double cell1Val) {
        	
        	//We are not looking for the matching keys.
			MapperKVBase.TablePartsCallback visitor =  tablePartsCallback;

			if (null != plugin) {
            	switch (this.mode) {
        		case MODE_COLS :
	    			visitor.onRowCols(cell1Key, cell1Val);
        			break;
        		case MODE_KEY :
	    			visitor.onRowKey(cell1Key);
        			break;
            	}
            }
        }
    }
    ///////////////////////////////////////////////////////////////////	
    Map<Double,BitSet> table = new HashMap<Double, BitSet>();

    public HSearchTableKVDoubleInverted() {
    }

    public byte[] toBytes() throws IOException {
    	 if (null == table) return null;
         Cell2<BitSet, Double> cell2 = new Cell2<BitSet, Double>(
         		SortedBytesBitset.getInstance(), SortedBytesDouble.getInstance());
         for (Map.Entry<Double, BitSet> entry: table.entrySet()) {
 			cell2.add(entry.getValue(),entry.getKey());
 		}
        cell2.sort(new CellComparator.DoubleComparator<BitSet>());
        return cell2.toBytesOnSortedData();
    }

    public void put(Integer key, Double value) {
    	if ( table.containsKey(value)) {
    		table.get(value).set(key);
    	} else {
    		BitSet bits = new BitSet();
    		bits.set(key);
    		table.put(value, bits);
    	}
    }

    @Override
    public void get(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public void keySet(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public void values(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public void keyValues(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private void iterate(byte[] input, HSearchQuery query, IHSearchPlugin pluginI, int mode) throws IOException, NumberFormatException {
    	
        MapperKVBase plugin = castPlugin(pluginI);
        MapperKVBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "Double"});

		cell2Visitor.matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Double matchingCell1 = ( query.filterCells[1] ) ? (Double) query.exactValCellsO[1]: null;


		cell2Visitor.cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		Double cellMin1 = null;


		cell2Visitor.cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		Double cellMax1 =  null;


		cell2Visitor.inValues0 =  (query.inValCells[0]) ? (Integer[])query.inValuesAO[0]: null;
		Double[] inValues1 =  (query.inValCells[1]) ? (Double[])query.inValuesAO[1]: null;

		Cell2<BitSet, Double> cell2 = new Cell2<BitSet, Double>(
        		SortedBytesBitset.getInstance(), SortedBytesDouble.getInstance());
        cell2.data = new BytesSection(input);
	
		if (query.filterCells[1]) {
			if(query.notValCells[1])
				cell2.processNot(matchingCell1, cell2Visitor);
			else if(query.inValCells[1])
				cell2.processIn(inValues1, cell2Visitor);
			else 
				cell2.process(matchingCell1, cellMin1, cellMax1,cell2Visitor);
		} else {
			cell2.process(cell2Visitor);
		}
		
        if (null != callback) {
            callback.onReadComplete();
        }
        if (null != plugin) {
            plugin.onReadComplete();
        }    	
    }

    public MapperKVBase castPlugin(IHSearchPlugin pluginI)
            throws IOException {
        MapperKVBase plugin = null;
        if (null != pluginI) {
            if (pluginI instanceof MapperKVBase) {
                plugin = (MapperKVBase) pluginI;
            }
            if (null == plugin) {
                throw new IOException("Invalid plugin Type :" + pluginI);
            }
        }
        return plugin;
    }

    /**
     * Free the cube data
     */
    public void clear() throws IOException {
        table.clear();
    }

}
