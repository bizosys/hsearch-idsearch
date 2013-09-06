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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.kv.MapperKVBase;
import com.bizosys.hsearch.kv.impl.ComputeFactory;
import com.bizosys.hsearch.kv.impl.ICompute;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;


public final class MapperKV extends MapperKVBase {

    public static final String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);

    HSearchProcessingInstruction instruction = null;

    ICompute compute = null;
    Set<Integer> ids = new HashSet<Integer>();
    
    @Override
    public final void setOutputType(final HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    	this.compute = ComputeFactory.getInstance().getCompute(this.instruction.getProcessingHint());
    	this.compute.setCallBackType(this.instruction.getOutputType());
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
    		result = SortedBytesInteger.getInstance().toBytes(this.ids);
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
        this.compute.clear();
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
        	computation.put(key, value);
            return true;
        }

		@Override
		public boolean onRowKey(int id) {
			this.whole.ids.add(id);
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
