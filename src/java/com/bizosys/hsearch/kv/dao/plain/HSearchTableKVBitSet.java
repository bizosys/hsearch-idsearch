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
package com.bizosys.hsearch.kv.dao.plain;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.BitSet;

import com.bizosys.hsearch.byteutils.ByteUtil;
import com.bizosys.hsearch.byteutils.SortedBytesBoolean;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.kv.dao.MapperKV;
import com.bizosys.hsearch.kv.dao.MapperKVBase;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell2Visitor;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.bizosys.hsearch.treetable.client.HSearchQuery;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

public final class HSearchTableKVBitSet implements IHSearchTable {

    public static boolean DEBUG_ENABLED = false;
    
    public static final int MODE_COLS = 0;
    public static final int MODE_KEY = 1;
    public static final int MODE_VAL = 2;
    public static final int MODE_KEYVAL = 3;

    public static final class Cell2FilterVisitor implements Cell2Visitor<Integer, Boolean> {

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
        public final void visit(final Integer cell1Key, final Boolean cell1Val) {
            			//Is it all or not.
			if (query.filterCells[0]) {
				
				//IS it exact or not
				if (null != matchingCell0) {
					if ( query.notValCells[0] ) {
						//Exact val match
						if (matchingCell0.intValue() == cell1Key.intValue()) return;
					} else {
						//Not Exact val 
						if (matchingCell0.intValue() != cell1Key.intValue()) return;
					}
				} else {
					//Either range or IN
					if ( query.inValCells[0]) {
						//IN
						boolean isMatched = false;
						//LOOKING FOR ONE MATCHING
						for ( Object obj : query.inValuesAO[0]) {
							Integer objI = (Integer) obj;
							isMatched = cell1Key.intValue() == objI.intValue();
							
							//ONE MATCHED, NO NEED TO PROCESS
							if ( query.notValCells[0] ) { 
								if (!isMatched ) break; 
							} else {
								if (isMatched ) break;
							}
						}
						if ( !isMatched ) return; //NONE MATCHED
						
					} else {
						//RANGE
						boolean isMatched = cell1Key.intValue() < cellMin0.intValue() || 
											cell1Key.intValue() > cellMax0.intValue();
						if ( query.notValCells[0] ) {
							//Not Exact Range
							if (!isMatched ) return;
						} else {
							//Exact Range
							if (isMatched ) return;
						}
					}
				}
			}
			
			System.out.println(cell1Key + " = " + cell1Val);
            if (null != plugin) {
            	switch (this.mode) {
        		case MODE_COLS :
        			tablePartsCallback.onRowCols(cell1Key, cell1Val);                                
        			break;
        		case MODE_KEY :
        			tablePartsCallback.onRowKey(cell1Key);
        			break;
            	}
            }
        }
    }
    ///////////////////////////////////////////////////////////////////	
    Cell2<Integer,Boolean> table = createBlankTable();

    public HSearchTableKVBitSet() {
    }

    public Cell2<Integer,Boolean> createBlankTable() {
        return new Cell2<Integer,Boolean>(SortedBytesInteger.getInstance(),
				SortedBytesBoolean.getInstance());
    }

    public final byte[] toBytes() throws IOException {

    	BitSet bits = new BitSet();

    	int minValue = 0;
    	int maxValue = 0;
    	for ( CellKeyValue<Integer, Boolean> cell : this.table.sortedList) {
    		if ( cell.key < minValue) minValue  = cell.key;
    		if ( cell.key > maxValue) maxValue  = cell.key;
		}
    	
    	int offset = 0 - minValue;
    	
    	for ( CellKeyValue<Integer, Boolean> cell : this.table.sortedList) {
			if ( cell.value ) bits.set( cell.key + offset);
			else bits.clear( cell.key + offset);
		}

		int available = maxValue + offset + 1;
		int packed = available/8;
		int remaining = available - packed * 8;
			
		int neededBytes = packed;
		if ( remaining > 0 ) neededBytes++;
			
		neededBytes = neededBytes + 4; 
		byte[] out = new byte[neededBytes];
			
		System.arraycopy(Storable.putInt(available), 0, out, 0, 4);
		
		int pointer = 0;
		for ( int i=0; i<packed; i++) {
			out[4+i]  = ByteUtil.fromBits(new boolean[] {
					bits.get(pointer++), bits.get(pointer++), bits.get(pointer++), bits.get(pointer++),
					bits.get(pointer++), bits.get(pointer++), bits.get(pointer++), bits.get(pointer++)});
		}
			
		if ( remaining > 0 ) {
			boolean[] remainingBits = new boolean[8];
			Arrays.fill(remainingBits, true);
			for ( int j=0; j<remaining; j++) {
				remainingBits[j] = bits.get(pointer++);
			}
				
			out[4+packed] = ByteUtil.fromBits(remainingBits);
		}
		byte[] outWithHeader = new byte[4 + out.length];
		System.arraycopy( Storable.putInt(offset) , 0, outWithHeader, 0, 4);
		System.arraycopy(out, 0, outWithHeader, 4, out.length);
		return outWithHeader;
    }

    public final void put(final Integer key, final Boolean value) {
        table.add(key, value);
    }

    @Override
    public final void get(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException, NumberFormatException {
    	iterate(input, query, pluginI, MODE_COLS);
    }

    @Override
    public final void keySet(final byte[] input, final HSearchQuery query, final IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEY);
    }

    public void values(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_VAL);
    }

    public void keyValues(byte[] input, HSearchQuery query, IHSearchPlugin pluginI) throws IOException {
    	iterate(input, query, pluginI, MODE_KEYVAL);
    }
    
    private void iterate(byte[] inputWithHeader, HSearchQuery query, IHSearchPlugin pluginI, int mode) throws IOException, NumberFormatException {
    	
        MapperKVBase plugin = castPlugin(pluginI);
        MapperKVBase.TablePartsCallback callback = plugin.getPart();

        Cell2FilterVisitor cell2Visitor = new Cell2FilterVisitor(query, pluginI, callback, mode);

        query.parseValuesConcurrent(new String[]{"Integer", "Boolean"});
		cell2Visitor.matchingCell0 = ( query.filterCells[0] ) ? (Integer) query.exactValCellsO[0]: null;
		Boolean matchingCell1 = ( query.filterCells[1] ) ? (Boolean) query.exactValCellsO[1]: null;

		cell2Visitor.inValues0 =  (query.inValCells[0]) ? (Integer[])query.inValuesAO[0]: null;
		Boolean[] inValues1 =  (query.inValCells[1]) ? (Boolean[])query.inValuesAO[1]: null;

		int offset = Storable.getInt(0, inputWithHeader);
		int available = Storable.getInt(4, inputWithHeader);
		int packed = available/8;
		int remaining = available - packed * 8;
		
		BitSet bitsWithOffset = new BitSet(available);
		int seq = 0;
		for (int i=0; i<packed; i++) {
			for (boolean val : Storable.byteToBits(inputWithHeader[8 + i])) {
				seq++;
				if ( available < seq) break;
				if ( val ) bitsWithOffset.set(offset + seq - 1);
			}
		}
		
		if ( remaining > 0 ) {
			boolean[] x = Storable.byteToBits(inputWithHeader[8 + packed]);
			for ( int i=0; i<remaining; i++) {
				seq++;
				if ( available < seq) break;
				if ( x[i] ) bitsWithOffset.set(offset + seq - 1);
 			}
		}
		
		
		if (query.filterCells[1]) {
			if(query.notValCells[1]) {
				for ( int i=0; i<available; i++) {
					if ( matchingCell1 == !bitsWithOffset.get(offset + i) ) cell2Visitor.visit(i, !matchingCell1);
				}
				
			}else if(query.inValCells[1]) {
				for ( boolean val: inValues1) {
					for ( int i=0; i<available; i++) {
						if ( val == bitsWithOffset.get(offset + i) ) cell2Visitor.visit(i - offset, val);
					}
				}
			}else {
				for ( int i=0; i<available; i++) {
					if ( matchingCell1 == bitsWithOffset.get(offset + i) ) cell2Visitor.visit(i - offset, matchingCell1);
				}
			}
		} else {
			for ( int i=0; i<available; i++) {
				cell2Visitor.visit(i - offset, bitsWithOffset.get(offset + i));
			}
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
        table.getMap().clear();
    }
    
    public static void main(String[] args) throws IOException, NumberFormatException, ParseException {
    	HSearchTableKVBitSet table = new HSearchTableKVBitSet();
    	table.put(1, true);
		table.put(5, true);
		table.put(7, true);
		table.put(-10, true);
		table.put(11, false);
		table.put(13, true);
		byte[] input = table.toBytes();
		HSearchQuery query = new HSearchQuery("*|false");
		table.iterate(input, query, new MapperKV(), 1);
	}
}
