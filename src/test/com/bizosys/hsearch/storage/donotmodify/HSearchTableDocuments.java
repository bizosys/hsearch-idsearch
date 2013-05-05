package com.bizosys.hsearch.storage.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShort;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.CellComparator.BytesComparator;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.unstructured.IIndexOffsetTable;
import com.bizosys.hsearch.util.EmptyMap;

public class HSearchTableDocuments implements IIndexOffsetTable  {
	
	public static boolean DEBUG_ENABLED = false;
	
	public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;
	
	
	public static final class Cell4Map
		 extends EmptyMap<Integer, Cell4<Integer, Integer, Integer, byte[]>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell1;
	public Integer cellMin1; 
	public Integer cellMax1;
	public Map<Integer, Cell3<Integer, Integer, byte[]>> cell3L = null;

	public Cell4Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,byte[] matchingCell4, byte[] cellMin4, byte[] cellMax4,Integer matchingCell2, Integer cellMin2, Integer cellMax2,Integer matchingCell1, Integer cellMin1, Integer cellMax1) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell1 = matchingCell1;
		this.cellMin1 = cellMin1;
		this.cellMax1 = cellMax1;
		this.cell3L = new Cell3Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4,matchingCell2, cellMin2, cellMax2);
	}
	@Override
	public Cell4<Integer, Integer, Integer, byte[]> put(Integer key, Cell4<Integer, Integer, Integer, byte[]> value) {
	try {
		cell2Visitor.cell4Key = key;
		if (query.filterCells[1]) {
			value.getMap(matchingCell1, cellMin1, cellMax1, cell3L);
		 } else {
			value.sortedList = cell3L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}


public static final class Cell3Map
		 extends EmptyMap<Integer, Cell3<Integer, Integer, byte[]>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell2;
	public Integer cellMin2; 
	public Integer cellMax2;
	public Map<Integer, Cell2<Integer, byte[]>> cell2L = null;

	public Cell3Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,byte[] matchingCell4, byte[] cellMin4, byte[] cellMax4,Integer matchingCell2, Integer cellMin2, Integer cellMax2) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell2 = matchingCell2;
		this.cellMin2 = cellMin2;
		this.cellMax2 = cellMax2;
		this.cell2L = new Cell2Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4);
	}
	@Override
	public Cell3<Integer, Integer, byte[]> put(Integer key, Cell3<Integer, Integer, byte[]> value) {
	try {
		cell2Visitor.cell3Key = key;
		if (query.filterCells[2]) {
			value.getMap(matchingCell2, cellMin2, cellMax2, cell2L);
		 } else {
			value.sortedList = cell2L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}



	
	public static final class Cell2Map extends EmptyMap<Integer, Cell2<Integer, byte[]>> {

		public HSearchQuery query;
		public byte[] matchingCell4;
		public byte[] cellMin4;
		public byte[] cellMax4;
		public Cell2FilterVisitor cell2Visitor;
		
		public Cell2Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,
			byte[] matchingCell4, byte[] cellMin4, byte[] cellMax4) {
			this.query = query;
			this.cell2Visitor = cell2Visitor;

			this.matchingCell4 = matchingCell4;
			this.cellMin4 = cellMin4;
			this.cellMax4 = cellMax4;
		}
		
		@Override
		public Cell2<Integer, byte[]> put(Integer key, Cell2<Integer, byte[]> value) {
			
			try {
				cell2Visitor.cell2Key = key;
				Cell2<Integer, byte[]> cell2Val = value;

				if (query.filterCells[4]) {
					cell2Val.process(matchingCell4, cellMin4, cellMax4,cell2Visitor);
				} else {
					cell2Val.process(cell2Visitor);
				}
				return value;
			} catch (IOException e) {
				throw new IndexOutOfBoundsException(e.getMessage());
			}
		}
	}
	
	///////////////////////////////////////////////////////////////////	
	
	public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, byte[]> {
		public HSearchQuery query;
		public IHSearchPlugin plugin;

		public PluginDocumentsBase.TablePartsCallback tablePartsCallback = null;

		public Integer matchingCell3;
		public Integer cellMin3;
		public Integer cellMax3;

		public int cell4Key;
		public int cell3Key;
		public int cell2Key;
		
		public int mode = MODE_COLS;
        public Cell2FilterVisitor(HSearchQuery query,IHSearchPlugin plugin, PluginDocumentsBase.TablePartsCallback tablePartsCallback, int mode) {
			
            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }
		
		public void set(Integer matchingCell3, Integer cellMin3, Integer cellMax3) {
			this.matchingCell3 = matchingCell3;
			this.cellMin3 = cellMin3;
			this.cellMax3 = cellMax3;
		}
		
		
		
		@Override
		public final void visit(Integer cell1Key, byte[] cell1Val) {
			if (query.filterCells[3]) {
				if (null != matchingCell3) {
					if (matchingCell3.intValue() != cell1Key.intValue()) return;
				} else {
					if (cell1Key.intValue() < cellMin3.intValue() || cell1Key.intValue() > cellMax3.intValue()) return;
				}
			}

			if (null != plugin) {
            	switch (this.mode) {
            		case MODE_COLS :
            			tablePartsCallback.onRowCols(cell4Key, cell3Key, cell2Key, cell1Key, cell1Val);
            			break;

            		case MODE_KEY :
            			tablePartsCallback.onRowKey(cell1Key);
            			break;
            		case MODE_KEYVAL :
            			tablePartsCallback.onRowKeyValue(cell1Key, cell1Val);
            			break;
            		case MODE_VAL:
            			tablePartsCallback.onRowValue(cell1Val);
            			break;
            	}
			} 
		}
	}	
	
	///////////////////////////////////////////////////////////////////	

	Cell5<Integer,Integer, Integer, Integer, byte[]> table = createBlankTable();

	public HSearchTableDocuments() {
	}
	
	public Cell5<Integer,Integer, Integer, Integer, byte[]> createBlankTable() {
		return new Cell5<Integer,Integer, Integer, Integer, byte[]>
			(
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) ,
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) ,
				SortedBytesInteger.getInstance(),
				SortedBytesInteger.getInstance(),
				SortedBytesArray.getInstanceArr()
			);
	}

	public byte[] toBytes() throws IOException {
		if ( null == table) return null;
		return table.toBytes(new BytesComparator<Integer>());
	}

	public void put (Integer doctype, Integer wordtype, Integer hashcode, Integer docid, byte[] positions) {
		table.put( doctype, wordtype, hashcode, docid, positions );
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
    	
        PluginDocumentsBase plugin = castPlugin(pluginI);
        PluginDocumentsBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "Integer", "Integer", "Integer", "byte[]"});

		Integer matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Integer matchingCell1 = ( query.filterCells[1] ) ? (Integer) query.exactValCellsO[1]: null;
		Integer matchingCell2 = ( query.filterCells[2] ) ? (Integer) query.exactValCellsO[2]: null;
		cell2Visitor.matchingCell3 = ( query.filterCells[3] ) ? (Integer) query.exactValCellsO[3]: null;
		 byte[] matchingCell4 = ( query.filterCells[4] ) ? (byte[]) query.exactValCellsO[4]: null;


		Integer cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		Integer cellMin1 = ( query.minValCells[1] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[1]).intValue();
		Integer cellMin2 = ( query.minValCells[2] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[2]).intValue();
		cell2Visitor.cellMin3 = ( query.minValCells[3] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[3]).intValue();
		 byte[] cellMin4 = null;


		Integer cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		Integer cellMax1 =  (query.maxValCells[1] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[1]).intValue();
		Integer cellMax2 =  (query.maxValCells[2] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[2]).intValue();
		cell2Visitor.cellMax3 =  (query.maxValCells[3] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[3]).intValue();
		 byte[] cellMax4 = null;


		Cell4Map cell4L = new Cell4Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4,matchingCell2, cellMin2, cellMax2,matchingCell1, cellMin1, cellMax1);

        this.table.data = new BytesSection(input);
        if (query.filterCells[0]) {
            this.table.getMap(matchingCell0, cellMin0, cellMax0, cell4L);
        } else {
            this.table.sortedList = cell4L;
            this.table.parseElements();
        }

        if (null != callback) callback.onReadComplete();
        if (null != plugin) plugin.onReadComplete();
    }

    public PluginDocumentsBase castPlugin(IHSearchPlugin pluginI)
            throws IOException {
        PluginDocumentsBase plugin = null;
        if (null != pluginI) {
            if (pluginI instanceof PluginDocumentsBase) {
                plugin = (PluginDocumentsBase) pluginI;
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
    	if ( null !=  table.sortedList) table.sortedList.clear();
    }

}
	