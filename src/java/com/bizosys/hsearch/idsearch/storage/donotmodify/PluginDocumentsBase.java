/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.bizosys.hsearch.idsearch.storage.donotmodify;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;
public abstract class PluginDocumentsBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
    
    public interface TablePartsCallback {
        boolean onRowKey(int id);
        public boolean onRowCols(int cell1, int cell2, int cell3, String cell4, int cell5, float cell6);
        public boolean onRowKeyValue(int key, float value);
        public boolean onRowValue(float value);
        public void onReadComplete();
    }
}
