/*
* Copyright 2010 Bizosys Technologies Limited
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
package com.bizosys.hsearch.embedded;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;

import com.bizosys.hsearch.embedded.donotmodify.PluginDocumentsBase;

public final class MapperDocuments extends PluginDocumentsBase {

	private static boolean DEBUG_MODE = EmbeddedHSearchLog.l.isDebugEnabled();
	@SuppressWarnings("unused")
	private static boolean INFO_MODE = EmbeddedHSearchLog.l.isInfoEnabled();
	
    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);

    HSearchProcessingInstruction instruction = null;
    BitSetOrSet bitsetOrSet = new BitSetOrSet();

    @Override
    public final void setOutputType(final HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    }

    /**
     * Merge is called when all threads finish their processing.
     * It can so happen that all threads may call at same time.
     * Maintain thread concurrency in the code.
     * Don't remove <code>this.parts.remove();</code> as after merging, it clears the ThreadLocal object. 
     */
    protected final void merge(Set<Integer> docIds) {
        if ( null == docIds) return;
        synchronized (this) {
            this.bitsetOrSet.setDocumentIds(docIds);
        }
        this.parts.remove();
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
        return this.bitsetOrSet;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public final void getResultSingleQuery(final Collection<byte[]> container) throws IOException {
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
    	this.bitsetOrSet.clear();
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

        public MapperDocuments whole = null;
        Set<Integer> docIds = new HashSet<Integer>();

        public RowReader(final MapperDocuments whole) {
            this.whole = whole;
        }

        public final boolean onRowKey(final int id) {
        	docIds.add(id);
            return true;
        }

        public final boolean onRowCols( final int doctype,  final int wordtype,  final int hashcode,  final int docid,  final int frequency) {
        	if ( DEBUG_MODE ) EmbeddedHSearchLog.l.debug(
        		doctype + "\t" + wordtype + "\t" + hashcode + "\t" + docid + "\t" + frequency);
        	
        	docIds.add(docid);
        	return true;
        }

        @Override
        public final boolean onRowKeyValue(final int key, final int value) {
            return true;
        }

        @Override
        public final boolean onRowValue(final int value) {
            return true;
        }

        @Override
        public final void onReadComplete() {
            this.whole.merge(this.docIds);
        }
    }

    /*******************************************************************************************
     * The below sections are generic in nature and no need to be changed.
     */
    /**
     * Do not modify this section as we need to create indivisual instances per thread.
     */
    public final ThreadLocal<PluginDocumentsBase.TablePartsCallback> parts = 
        	new ThreadLocal<PluginDocumentsBase.TablePartsCallback>();
    @Override
    public final PluginDocumentsBase.TablePartsCallback getPart() {
        PluginDocumentsBase.TablePartsCallback part = parts.get();
        if (null == part) {
            parts.set(new RowReader(this));
            return parts.get();
        } 
        return part;
    }

    /**
     * sample ser/deser. Use Storable class.
     public static byte[] ser(int findings, int groups, int studies) {
    	byte[] output = new byte[12];
        System.arraycopy(Storable.putInt(cell1), 0, output, 0, 4);
        System.arraycopy(Storable.putFloat(cell2), 0, output, 4, 4);
        return output;
    }
     */
}
