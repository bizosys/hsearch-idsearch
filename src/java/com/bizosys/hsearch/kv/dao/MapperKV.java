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

package com.bizosys.hsearch.kv.dao;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.SortedBytesBitset;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.kv.impl.ComputeFactory;
import com.bizosys.hsearch.kv.impl.ICompute;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

/**
 * 
 * A mapper class that maps the returned rows for a given query
 * and sends it for aggregation.
 *
 */
public final class MapperKV extends MapperKVBase {

    static boolean DEBUG_ENABLED = false;

    HSearchProcessingInstruction instruction = null;

    public ICompute compute = null;
    public BitSetWrapper matchingIds = null;
    public BitSetWrapper returnIds = new BitSetWrapper();
    
    /**
     * Sets the hsearch instruction type that needs to be used for a search. 
     */
    @Override
    public final void setOutputType(final HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    	this.compute = ComputeFactory.getInstance().getCompute(this.instruction.getProcessingHint());
    	this.compute.setCallBackType(this.instruction.getOutputType());
    }

    /**
     * Sets the matching ids.
     */
	@Override
	public final void setMergeId(final byte[] matchingIds) throws IOException {
		this.matchingIds = SortedBytesBitset.getInstanceBitset().bytesToBitSet(matchingIds, 0, matchingIds.length);
	}
	
	
    /**
     * When all parts are completed, finally it is called.
     * By this time, the result of all parts is available for final processing.
     * 
     */
    @Override
    public final void onReadComplete() {
    }


    /**
     * For multi queries, we need to provide matching documents for 
     * intersection. For sinle query this is having no usage and can be passed null to save computing.
     *     	BitSetOrSet sets = new BitSetOrSet();  sets.setDocumentIds(this.rows.keySet());
     *      OR,
     *      Set Document Positions. 
     */
    @Override
    public final BitSetOrSet getUniqueMatchingDocumentIds() throws IOException {
        return null;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {
    	byte[] result = null;
    	if(this.instruction.getCallbackType() == HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS){
    		result = this.compute.toBytes();
    		if(null != result)container.add(result);    		
    	}
    	else{
    		result = SortedBytesBitset.getInstanceBitset().bitSetToBytes(returnIds);
    		if(null != result)container.add(result);
    	}
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultMultiQuery(final BitSetOrSet matchedIds, final Collection<byte[]> container) throws IOException {
    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public final void clear() {
    	if(null != compute)
    		compute.clear();
    	if(null != matchingIds)
    		matchingIds.clear();
    	if(null != returnIds)
    		returnIds.clear();
    }

    /**
     * Each thread will have 1 instance of this class.
     * For all found rows, the onRowCols/onRowKeys/... are called as we have set in the instructor.
     * @see Client.execute
     * When all are processed, this reports back to the main class for agrregating the result via merge.
     * @see Mapper.whole.merge
     * @author abinash
     *
     */
    public static final class RowReader implements TablePartsCallback {

        public MapperKV whole = null;
        ICompute computation = null;
        public RowReader(final MapperKV whole) {
            this.whole = whole;
        	this.computation = whole.compute;
        }

        public final boolean onRowCols( final int key,  final Object value) {
        	if(this.whole.matchingIds.get(key))
        		computation.put(key, value);
            return true;
        }

		@Override
		public boolean onRowKey(int id) {
			this.whole.returnIds.set(id);
			return true;
		}

        @Override
		public boolean onRowKey(BitSetWrapper ids) {
        	this.whole.returnIds.or(ids);
			return true;
		}

		@Override
		public boolean onRowCols(BitSetWrapper ids, Object value) {
			ids.and(this.whole.matchingIds);
        	for (int id = ids.nextSetBit(0); id >= 0; id = ids.nextSetBit(id+1)) {
        		computation.put(id, value);
        	}
			return true;
		}

        @Override
        public final void onReadComplete() {
        }
    }

    @Override
    public final MapperKVBase.TablePartsCallback getPart() {
        return new RowReader(this);
    }
}
