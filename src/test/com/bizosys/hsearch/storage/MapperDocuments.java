package com.bizosys.hsearch.storage;

import java.io.IOException;
import java.util.Collection;

import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.BitSetOrSet;
import com.bizosys.hsearch.storage.donotmodify.PluginDocumentsBase;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;


public class MapperDocuments extends PluginDocumentsBase {

    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);

    HSearchProcessingInstruction instruction = null;

    @Override
    public void setOutputType(HSearchProcessingInstruction outputTypeCode) {
        this.instruction = outputTypeCode;
    }

    /**
     * Merge is called when all threads finish their processing.
     * It can so happen that all threads may call at same time.
     * Maintain thread concurrency in the code.
     * Don't remove <code>this.parts.remove();</code> as after merging, it clears the ThreadLocal object. 
     */
    protected void merge(/** attributes as needed */) {
        synchronized (this) {
            //Computation goes here
        }
        this.parts.remove();
    }

    /**
     * When all parts are completed, finally it is called.
     * By this time, the result of all parts is available for final processing.
     * 
     */
    @Override
    public void onReadComplete() {
    }


    /**
     * For multi queries, we need to provide matching documents for 
     * intersection. For sinle query this is having no usage and can be passed null to save computing.
     *     	BitSetOrSet sets = new BitSetOrSet();  sets.setDocumentIds(this.rows.keySet());
     *      OR,
     *      Set Document Positions. 
     */
    @Override
    public BitSetOrSet getUniqueMatchingDocumentIds() throws IOException {
        return null;
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public void getResultSingleQuery(Collection<byte[]> container) throws IOException {
    }

    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public void getResultMultiQuery(BitSetOrSet matchedIds, Collection<byte[]> container) throws IOException {
    }

    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public void clear() {
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
    public static class RowReader implements TablePartsCallback {

        public MapperDocuments whole = null;

        public RowReader(MapperDocuments whole) {
            this.whole = whole;
        }

        public final boolean onRowKey(int id) {
            return true;
        }

        public final boolean onRowCols(int cell1, int cell2, int cell3, int cell4, byte[] cell5) {
            System.out.println(cell1 + "\t" + cell2 + "\t" + cell3 + "\t" + cell4 + "\t" + cell5);
        	return true;
        }

        @Override
        public final boolean onRowKeyValue(int key, byte[] value) {
            return true;
        }

        @Override
        public final boolean onRowValue(byte[] value) {
            return true;
        }

        @Override
        public void onReadComplete() {
            this.whole.merge( /** attributes as needed */ );
            /**
             * Clean up resources for reuse.
             */
        }
    }

    /*******************************************************************************************
     * The below sections are generic in nature and no need to be changed.
     */
    /**
     * Do not modify this section as we need to create indivisual instances per thread.
     */
    public ThreadLocal<PluginDocumentsBase.TablePartsCallback> parts = 
        	new ThreadLocal<PluginDocumentsBase.TablePartsCallback>();
    @Override
    public PluginDocumentsBase.TablePartsCallback getPart() {
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
