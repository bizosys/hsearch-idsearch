/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.storage.donotmodify;

import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class PluginDocumentsBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
    
    public interface TablePartsCallback {

        boolean onRowKey(int id);
        public boolean onRowCols(int cell1, int cell2, int cell3, int cell4, byte[] cell5);
        public boolean onRowKeyValue(int key, byte[] value);
        public boolean onRowValue(byte[] value);
        public void onReadComplete();
    }
}
