/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.embedded.donotmodify;

import java.io.IOException;

import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class PluginDocumentsBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
    
	@Override
	public final void setMergeId(final byte[] mergeId) throws IOException {
	}
	
    public interface TablePartsCallback {

        boolean onRowKey(int id);
        public boolean onRowCols( final int doctype,  final int wordtype,  final int hashcode,  final int docid,  final int frequency);
        public boolean onRowKeyValue(int key, int value);
        public boolean onRowValue(int value);
        public void onReadComplete();
    }
}
