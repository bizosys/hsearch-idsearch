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
package com.bizosys.hsearch.embedded.donotmodify;

import java.io.IOException;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.SortedBytesUnsignedShort;
import com.bizosys.hsearch.treetable.BytesSection;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.CellComparator.IntegerComparator;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.unstructured.IIndexFrequencyTable;
import com.bizosys.hsearch.util.EmptyMap;

public final class HSearchTableDocuments implements IIndexFrequencyTable {
	
	public static boolean DEBUG_ENABLED = false;
	
	public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;
	
	
	public static final class Cell4Map
		 extends EmptyMap<Integer, Cell4<Integer, Integer, Integer, Integer>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell1;
	public Integer cellMin1; 
	public Integer cellMax1;
	public Integer[] inValues1;
	public Map<Integer, Cell3<Integer, Integer, Integer>> cell3L = null;

	public Cell4Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Integer matchingCell4, final Integer cellMin4, final Integer cellMax4, final Integer[] inValues4, final Integer matchingCell2, final Integer cellMin2, final Integer cellMax2, final Integer[] inValues2, final Integer matchingCell1, final Integer cellMin1, final Integer cellMax1, final Integer[] inValues1) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell1 = matchingCell1;
		this.cellMin1 = cellMin1;
		this.cellMax1 = cellMax1;
		this.inValues1 = inValues1;
		this.cell3L = new Cell3Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4, inValues4,matchingCell2, cellMin2, cellMax2, inValues2);
	}
	@Override
	public final Cell4<Integer, Integer, Integer, Integer> put( final Integer key, final Cell4<Integer, Integer, Integer, Integer> value) {
	try {
		cell2Visitor.cell4Key = key;
		if (query.filterCells[1]) {
			if (query.notValCells[1])
				value.getNotMap(matchingCell1, cell3L);
			else if (query.inValCells[1])
				value.getInMap(inValues1, cell3L);
			else
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
		 extends EmptyMap<Integer, Cell3<Integer, Integer, Integer>> {

	public HSearchQuery query;
	public Cell2FilterVisitor cell2Visitor;
	public Integer matchingCell2;
	public Integer cellMin2; 
	public Integer cellMax2;
	public Integer[] inValues2;
	public Map<Integer, Cell2<Integer, Integer>> cell2L = null;

	public Cell3Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor, final Integer matchingCell4, final Integer cellMin4, final Integer cellMax4, final Integer[] inValues4, final Integer matchingCell2, final Integer cellMin2, final Integer cellMax2, final Integer[] inValues2) {
		this.query = query; 
		this.cell2Visitor = cell2Visitor;
		this.matchingCell2 = matchingCell2;
		this.cellMin2 = cellMin2;
		this.cellMax2 = cellMax2;
		this.inValues2 = inValues2;
		this.cell2L = new Cell2Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4, inValues4);
	}
	@Override
	public final Cell3<Integer, Integer, Integer> put( final Integer key, final Cell3<Integer, Integer, Integer> value) {
	try {
		cell2Visitor.cell3Key = key;
		if (query.filterCells[2]) {
			if (query.notValCells[2])
				value.getNotMap(matchingCell2, cell2L);
			else if (query.inValCells[2])
				value.getInMap(inValues2, cell2L);
			else
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



	
	public static final class Cell2Map extends EmptyMap<Integer, Cell2<Integer, Integer>> {

		public HSearchQuery query;
		public Integer matchingCell4;
		public Integer cellMin4;
		public Integer cellMax4;
		public Integer[] inValues4;
		public Cell2FilterVisitor cell2Visitor;
		
		public Cell2Map(final HSearchQuery query, final Cell2FilterVisitor cell2Visitor,
			final Integer matchingCell4, final Integer cellMin4, final Integer cellMax4, final Integer[] inValues4) {
			this.query = query;
			this.cell2Visitor = cell2Visitor;

			this.matchingCell4 = matchingCell4;
			this.cellMin4 = cellMin4;
			this.cellMax4 = cellMax4;
			this.inValues4 = inValues4;
		}
		
		@Override
		public final Cell2<Integer, Integer> put(final Integer key, final Cell2<Integer, Integer> value) {
			
			try {
				cell2Visitor.cell2Key = key;
				Cell2<Integer, Integer> cell2Val = value;

				if (query.filterCells[4]) {
					if(query.notValCells[4])
						cell2Val.processNot(matchingCell4, cell2Visitor);
					else if(query.inValCells[4])
						cell2Val.processIn(inValues4, cell2Visitor);
					else 
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
	
	public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, Integer> {
		public HSearchQuery query;
		public IHSearchPlugin plugin;

		public PluginDocumentsBase.TablePartsCallback tablePartsCallback = null;

		public Integer matchingCell3;
		public Integer cellMin3;
		public Integer cellMax3;
		public Integer[] inValues3;

		public int cell4Key;
		public int cell3Key;
		public int cell2Key;
		
		public int mode = MODE_COLS;
        public Cell2FilterVisitor(final HSearchQuery query, final IHSearchPlugin plugin, final PluginDocumentsBase.TablePartsCallback tablePartsCallback, final int mode) {
			
            this.query = query;
            this.plugin = plugin;
            this.tablePartsCallback = tablePartsCallback;
            this.mode = mode;

        }
		
		public final void set(final Integer matchingCell3, final Integer cellMin3, final Integer cellMax3, final Integer[] inValues3) {
			this.matchingCell3 = matchingCell3;
			this.cellMin3 = cellMin3;
			this.cellMax3 = cellMax3;
			this.inValues3 = inValues3;
		}
		
		
		
		@Override
		public final void visit(final Integer cell1Key, final Integer cell1Val) {

			//Is it all or not.
			if (query.filterCells[3]) {
				
				//IS it exact or not
				if (null != matchingCell3) {
					if ( query.notValCells[3] ) {
						//Exact val match
						if (matchingCell3.intValue() == cell1Key.intValue()) return;
					} else {
						//Not Exact val 
						if (matchingCell3.intValue() != cell1Key.intValue()) return;
					}
				} else {
					//Either range or IN
					if ( query.inValCells[3]) {
						//IN
						boolean isMatched = false;
						//LOOKING FOR ONE MATCHING
						for ( Object obj : query.inValuesAO[3]) {
							Integer objI = (Integer) obj;
							isMatched = cell1Key.intValue() == objI.intValue();
							
							//ONE MATCHED, NO NEED TO PROCESS
							if ( query.notValCells[3] ) { 
								if (!isMatched ) break; 
							} else {
								if (isMatched ) break;
							}
						}
						if ( !isMatched ) return; //NONE MATCHED
						
					} else {
						//RANGE
						boolean isMatched = cell1Key.intValue() < cellMin3.intValue() || 
											cell1Key.intValue() > cellMax3.intValue();
						if ( query.notValCells[3] ) {
							//Not Exact Range
							if (!isMatched ) return;
						} else {
							//Exact Range
							if (isMatched ) return;
						}
					}
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

	Cell5<Integer,Integer, Integer, Integer, Integer> table = createBlankTable();

	public HSearchTableDocuments() {
	}
	
	public final Cell5<Integer,Integer, Integer, Integer, Integer> createBlankTable() {
		return new Cell5<Integer,Integer, Integer, Integer, Integer>
			(
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) ,
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) ,
				SortedBytesInteger.getInstance(),
				SortedBytesInteger.getInstance(),
				SortedBytesUnsignedShort.getInstanceShort().setMinimumValueLimit((short) -32768.0 ) 
			);
	}

	public final byte[] toBytes() throws IOException {
		if ( null == table) return null;
		return table.toBytes(new IntegerComparator<Integer>());
	}

	public final void put (Integer doctype, Integer wordtype, Integer hashcode, Integer docid, Integer frequency) {
		table.put( doctype, wordtype, hashcode, docid, frequency );
	}
	
    @Override
    public final void get(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public final void keySet(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public final void values(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public final void keyValues(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private final void iterate(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI, final int mode) throws IOException, NumberFormatException {
    	
        PluginDocumentsBase plugin = castPlugin(pluginI);
        PluginDocumentsBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "Integer", "Integer", "Integer", "Integer"});

		Integer matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Integer matchingCell1 = ( query.filterCells[1] ) ? (Integer) query.exactValCellsO[1]: null;
		Integer matchingCell2 = ( query.filterCells[2] ) ? (Integer) query.exactValCellsO[2]: null;
		cell2Visitor.matchingCell3 = ( query.filterCells[3] ) ? (Integer) query.exactValCellsO[3]: null;
		Integer matchingCell4 = ( query.filterCells[4] ) ? (Integer) query.exactValCellsO[4]: null;


		Integer cellMin0 = ( query.minValCells[0] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[0]).intValue();
		Integer cellMin1 = ( query.minValCells[1] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[1]).intValue();
		Integer cellMin2 = ( query.minValCells[2] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[2]).intValue();
		cell2Visitor.cellMin3 = ( query.minValCells[3] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[3]).intValue();
		Integer cellMin4 = ( query.minValCells[4] == HSearchQuery.DOUBLE_MIN_VALUE) ? null : new Double(query.minValCells[4]).intValue();


		Integer cellMax0 =  (query.maxValCells[0] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[0]).intValue();
		Integer cellMax1 =  (query.maxValCells[1] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[1]).intValue();
		Integer cellMax2 =  (query.maxValCells[2] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[2]).intValue();
		cell2Visitor.cellMax3 =  (query.maxValCells[3] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[3]).intValue();
		Integer cellMax4 =  (query.maxValCells[4] == HSearchQuery.DOUBLE_MAX_VALUE) ? null : new Double(query.maxValCells[4]).intValue();


		Integer[] inValues0 =  (query.inValCells[0]) ? (Integer[])query.inValuesAO[0]: null;
		Integer[] inValues1 =  (query.inValCells[1]) ? (Integer[])query.inValuesAO[1]: null;
		Integer[] inValues2 =  (query.inValCells[2]) ? (Integer[])query.inValuesAO[2]: null;
		cell2Visitor.inValues3 =  (query.inValCells[3]) ? (Integer[])query.inValuesAO[3]: null;
		Integer[] inValues4 =  (query.inValCells[4]) ? (Integer[])query.inValuesAO[4]: null;


		Cell4Map cell4L = new Cell4Map(query, cell2Visitor,matchingCell4, cellMin4, cellMax4, inValues4,matchingCell2, cellMin2, cellMax2, inValues2,matchingCell1, cellMin1, cellMax1, inValues1);

        this.table.data = new BytesSection(input);
        if (query.filterCells[0]) {
			if(query.notValCells[0])
				this.table.getNotMap(matchingCell0, cell4L);
			else if(query.inValCells[0])
				this.table.getInMap(inValues0, cell4L);
			else	
				this.table.getMap(matchingCell0, cellMin0, cellMax0, cell4L);
        } else {
            this.table.sortedList = cell4L;
            this.table.parseElements();
        }

        if (null != callback) callback.onReadComplete();
        if (null != plugin) plugin.onReadComplete();
    }

    public final PluginDocumentsBase castPlugin(final IHSearchPlugin pluginI)
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
    public final void clear() throws IOException {
        if ( null !=  table.sortedList) table.sortedList.clear();
    }

}
	