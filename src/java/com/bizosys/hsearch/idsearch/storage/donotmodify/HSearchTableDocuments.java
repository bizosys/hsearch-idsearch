package com.bizosys.hsearch.idsearch.storage.donotmodify;
import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShort;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.Cell6;
import com.bizosys.hsearch.treetable.CellComparator.FloatComparator;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;
import com.bizosys.hsearch.util.EmptyMap;

public class HSearchTableDocuments implements IHSearchTable {
	
	public static boolean DEBUG_ENABLED = false;
	
	public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;
	
	
	public static final class Cell5Map
		 extends EmptyMap<Integer, Cell5<Integer, Integer, String, Integer, Float>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell1;
	public Integer cellMin1; 
	public Integer cellMax1;
	public Map<Integer, Cell4<Integer, String, Integer, Float>> cell4L = null;

	public Cell5Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,Float matchingCell5, Float cellMin5, Float cellMax5,String matchingCell3, String cellMin3, String cellMax3,Integer matchingCell2, Integer cellMin2, Integer cellMax2,Integer matchingCell1, Integer cellMin1, Integer cellMax1) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell1 = matchingCell1;
		this.cellMin1 = cellMin1;
		this.cellMax1 = cellMax1;
		this.cell4L = new Cell4Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5,matchingCell3, cellMin3, cellMax3,matchingCell2, cellMin2, cellMax2);
	}
	@Override
	public Cell5<Integer, Integer, String, Integer, Float> put(Integer key, Cell5<Integer, Integer, String, Integer, Float> value) {
	try {
		cell2Visitor.cell5Key = key;
		if (query.filterCells[1]) {
			value.getMap(matchingCell1, cellMin1, cellMax1, cell4L);
		 } else {
			value.sortedList = cell4L;
			value.parseElements();
		}
		return value;
		} catch (IOException e) {
			throw new IndexOutOfBoundsException(e.getMessage());
		}
	}
}


public static final class Cell4Map
		 extends EmptyMap<Integer, Cell4<Integer, String, Integer, Float>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell2;
	public Integer cellMin2; 
	public Integer cellMax2;
	public Map<Integer, Cell3<String, Integer, Float>> cell3L = null;

	public Cell4Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,Float matchingCell5, Float cellMin5, Float cellMax5,String matchingCell3, String cellMin3, String cellMax3,Integer matchingCell2, Integer cellMin2, Integer cellMax2) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell2 = matchingCell2;
		this.cellMin2 = cellMin2;
		this.cellMax2 = cellMax2;
		this.cell3L = new Cell3Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5,matchingCell3, cellMin3, cellMax3);
	}
	@Override
	public Cell4<Integer, String, Integer, Float> put(Integer key, Cell4<Integer, String, Integer, Float> value) {
	try {
		cell2Visitor.cell4Key = key;
		if (query.filterCells[2]) {
			value.getMap(matchingCell2, cellMin2, cellMax2, cell3L);
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
		 extends EmptyMap<Integer, Cell3<String, Integer, Float>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public String matchingCell3;
	public String cellMin3; 
	public String cellMax3;
	public Map<String, Cell2<Integer, Float>> cell2L = null;

	public Cell3Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,Float matchingCell5, Float cellMin5, Float cellMax5,String matchingCell3, String cellMin3, String cellMax3) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell3 = matchingCell3;
		this.cellMin3 = cellMin3;
		this.cellMax3 = cellMax3;
		this.cell2L = new Cell2Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5);
	}
	@Override
	public Cell3<String, Integer, Float> put(Integer key, Cell3<String, Integer, Float> value) {
	try {
		cell2Visitor.cell3Key = key;
		if (query.filterCells[3]) {
			value.getMap(matchingCell3, cellMin3, cellMax3, cell2L);
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



	
	public static final class Cell2Map extends EmptyMap<String, Cell2<Integer, Float>> {
		public HSearchQuery query;
		public Float matchingCell5;
		public Float cellMin5;
		public Float cellMax5;
		public Cell2FilterVisitor cell2Visitor;
		
		public Cell2Map(HSearchQuery query, Cell2FilterVisitor cell2Visitor,
			Float matchingCell5, Float cellMin5, Float cellMax5) {
			this.query = query;
			this.cell2Visitor = cell2Visitor;
			this.matchingCell5 = matchingCell5;
			this.cellMin5 = cellMin5;
			this.cellMax5 = cellMax5;
		}
		
		@Override
		public Cell2<Integer, Float> put(String key, Cell2<Integer, Float> value) {
			
			try {
				cell2Visitor.cell2Key = key;
				Cell2<Integer, Float> cell2Val = value;
				if (query.filterCells[5]) {
					cell2Val.process(matchingCell5, cellMin5, cellMax5,cell2Visitor);
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
	
	public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, Float> {
		public HSearchQuery query;
		public IHSearchPlugin plugin;
		public PluginDocumentsBase.TablePartsCallback tablePartsCallback = null;
		public Integer matchingCell4;
		public Integer cellMin4;
		public Integer cellMax4;
		public int cell5Key;
		public int cell4Key;
		public int cell3Key;
		public String cell2Key;
		
		public int mode = MODE_COLS;
        public Cell2FilterVisitor(HSearchQuery query,IHSearchPlugin plugin, PluginDocumentsBase.TablePartsCallback tablePartsCallback, int mode) {
			
            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;
        }
		
		public void set(Integer matchingCell4, Integer cellMin4, Integer cellMax4) {
			this.matchingCell4 = matchingCell4;
			this.cellMin4 = cellMin4;
			this.cellMax4 = cellMax4;
		}
		
		
		
		@Override
		public final void visit(Integer cell1Key, Float cell1Val) {
			if (query.filterCells[4]) {
				if (null != matchingCell4) {
					if (matchingCell4.intValue() != cell1Key.intValue()) return;
				} else {
					if (cell1Key.intValue() < cellMin4.intValue() || cell1Key.intValue() > cellMax4.intValue()) return;
				}
			}
			if (null != plugin) {
            	switch (this.mode) {
            		case MODE_COLS :
            			tablePartsCallback.onRowCols(cell5Key, cell4Key, cell3Key, cell2Key, cell1Key, cell1Val);
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
	Cell6<Integer,Integer, Integer, String, Integer, Float> table = createBlankTable();
	public HSearchTableDocuments() {
	}
	
	public Cell6<Integer,Integer, Integer, String, Integer, Float> createBlankTable() {
		return new Cell6<Integer,Integer, Integer, String, Integer, Float>
			(
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -1.0 ) ,
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -1.0 ) ,
				SortedBytesInteger.getInstance(),
				SortedBytesString.getInstance(),
				SortedBytesInteger.getInstance(),
				SortedBytesFloat.getInstance()
			);
	}
	public byte[] toBytes() throws IOException {
		if ( null == table) return null;
		return table.toBytes(new FloatComparator<Integer>());
	}
	public void put (Integer doctype, Integer wordtype, Integer hashcode, String term, Integer docid, Float weight) {
		table.put( doctype, wordtype, hashcode, term, docid, weight );
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
        query.parseValuesConcurrent(new String[]{"Integer", "Integer", "Integer", "String", "Integer", "Float"});
		Integer matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Integer matchingCell1 = ( query.filterCells[1] ) ? (Integer) query.exactValCellsO[1]: null;
		Integer matchingCell2 = ( query.filterCells[2] ) ? (Integer) query.exactValCellsO[2]: null;
		String matchingCell3 = ( query.filterCells[3] ) ? (String) query.exactValCellsO[3]: null;
		cell2Visitor.matchingCell4 = ( query.filterCells[4] ) ? (Integer) query.exactValCellsO[4]: null;
		Float matchingCell5 = ( query.filterCells[5] ) ? (Float) query.exactValCellsO[5]: null;

		Integer cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		Integer cellMin1 = ( query.minValCells[1] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[1]).intValue();
		Integer cellMin2 = ( query.minValCells[2] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[2]).intValue();
		String cellMin3 = null;
		cell2Visitor.cellMin4 = ( query.minValCells[4] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[4]).intValue();
		Float cellMin5 = ( query.minValCells[5] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[5]).floatValue();

		Integer cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		Integer cellMax1 =  (query.maxValCells[1] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[1]).intValue();
		Integer cellMax2 =  (query.maxValCells[2] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[2]).intValue();
		String cellMax3 = null;
		cell2Visitor.cellMax4 =  (query.maxValCells[4] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[4]).intValue();
		Float cellMax5 =  (query.maxValCells[5] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[5]).floatValue();

		Cell5Map cell5L = new Cell5Map(query, cell2Visitor,matchingCell5, cellMin5, cellMax5,matchingCell3, cellMin3, cellMax3,matchingCell2, cellMin2, cellMax2,matchingCell1, cellMin1, cellMax1);
        this.table.data = new BytesSection(input);
        if (query.filterCells[0]) {
            this.table.getMap(matchingCell0, cellMin0, cellMax0, cell5L);
        } else {
            this.table.sortedList = cell5L;
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
        table.getMap().clear();
    }
    
    public static void main(String[] args) throws Exception {
    	HSearchTableDocuments ser = new HSearchTableDocuments();
    	ser.put(1, 1, "abinash".hashCode(), "abinash", 1, 1.0F);
    	
    	new HSearchTableDocuments().get(ser.toBytes(), new HSearchQuery("*|abinash|*|*|*|*"), 
    			new com.bizosys.hsearch.idsearch.storage.MapperDocuments());
    }
}
	
