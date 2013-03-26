package com.bizosys.hsearch.idsearch.storage;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesBase.Reference;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesString;
import com.bizosys.hsearch.byteutils.Storable;
import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.idsearch.storage.donotmodify.PluginDocumentsBase;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
public class MapperDocuments extends PluginDocumentsBase {
    public static String EMPTY = "";
    static boolean DEBUG_ENABLED = false;
    static byte[] bytesFor1 = Storable.putInt(1);
    
    HSearchProcessingInstruction instruction = null;
    
	Map<String, Float> docs = new HashMap<String, Float>();    
    
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
     */
    @Override
    public Collection<String> getUniqueMatchingDocumentIds() throws IOException {
		return docs.keySet();
    }
    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public void getResultSingleQuery(Collection<byte[]> container) throws IOException {
    	ser(container, docs);
    }
    /**
     * Collects the results for rows level aggregation.
     */
    @Override
    public void getResultMultiQuery(List<FederatedFacade<String, String>.IRowId> matchedIds,
            Collection<byte[]> container) throws IOException {

		Map<String, Float> finalRows = new HashMap<String, Float>();
		
		String docId = null;
		for (FederatedFacade<String, String>.IRowId row : matchedIds) {
			docId = row.getDocId();
			if ( !docs.containsKey(docId)) continue;
			finalRows.put(docId, docs.get(docId));
		}
		
		ser(container, docs);
		finalRows.clear();
		
    }
    
    /**
     * This method is finally called which cleans up all the resources.
     * If not cleaned up the results will be contaminated
     */
    @Override
    public void clear() {
		docs.clear();
    }
    
    protected void merge(Map<String, Float> docs) {
        synchronized (this) {
        	this.docs.putAll(docs);
        }
        this.parts.remove();
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
        Map<String, Float> docs = new HashMap<String, Float>();
        
        public RowReader(MapperDocuments whole) {
            this.whole = whole;
        }
        public final boolean onRowKey(int id) {
            return true;
        }
        
        public final boolean onRowCols(int cell3, int cell4, int cell1, String cell2, int cell5, float cell6) {
        	System.out.println("\n\n******" + new Integer(cell5).toString() + "-----" +  cell6 + "\n\n");
        	docs.put(new Integer(cell5).toString(), cell6);
        	return true;
        }
        
        @Override
        public final boolean onRowKeyValue(int key, float value) {
            return true;
        }
        
        @Override
        public final boolean onRowValue(float value) {
            return true;
        }
        
        @Override
        public void onReadComplete() {
            this.whole.merge(docs);
            docs.clear();
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
     */
	public static void ser(Collection<byte[]> output, Map<String, Float> docs) throws IOException {
		ISortedByte<String> keys = SortedBytesString.getInstance();
		ISortedByte<Float> vals = SortedBytesFloat.getInstance();
		
		byte[] serBytes = SortedBytesArray.getInstanceArr().toBytes( 
			keys.toBytes(docs.keySet()), vals.toBytes(docs.values()));
		
		output.add(serBytes);
	}
     
	public static void deser(Collection<byte[]> output, Map<String, Float> docs) throws IOException {
		
		SortedBytesArray sba = SortedBytesArray.getInstanceArr();
		ISortedByte<String> keys = SortedBytesString.getInstance();
		ISortedByte<Float> vals = SortedBytesFloat.getInstance();
		Reference keyRef = new Reference();
		Reference valRef = new Reference();
		
		for (byte[] data : output) {
			if ( null == data) continue;
			
			sba.parse(data);
			if ( sba.getSize() == 0 ) continue;
			
			if ( sba.getSize() != 2 ) {
				throw new IOException ("Corrupted deserialization : " + sba.getSize());
			}
			sba.getValueAtReference(0, keyRef);
			sba.getValueAtReference(1, valRef);
			
			keys.parse(data, keyRef.offset, keyRef.length);
			vals.parse(data, valRef.offset, valRef.length);
			
			int keysT = keys.getSize();
			int valsT = vals.getSize();
			
			if ( keysT != valsT) throw new IOException (
				"Keys (" + keysT + ") and values (" +  valsT + ")mismatched ." );
			
			for ( int i=0; i<keysT; i++) {
				docs.put(keys.getValueAt(i), vals.getValueAt(i));
			}
		}
	}

}
