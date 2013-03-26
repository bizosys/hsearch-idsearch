package com.bizosys.hsearch.idsearch.storage;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.idsearch.storage.donotmodify.HBaseTableSchema;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.storage.HSearchGenericFilter;
import com.bizosys.hsearch.treetable.storage.HSearchTableReader;

public class Client extends HSearchTableReader {

	HSearchGenericFilter filter = null;
	
	Map<String, Float> output = null;
	
    public Client(Map<String, Float> output) throws IOException {
        HBaseTableSchema.getInstance();
        this.output = output;
    }
        
    @Override
    public HSearchGenericFilter getFilter(String multiQuery,
            Map<String, String> multiQueryParts, HSearchProcessingInstruction outputType) {
        filter = new Filter(outputType, multiQuery, multiQueryParts);
        return filter;
    }
    
    @Override
    public void rows(Map<byte[], byte[]> results, HSearchProcessingInstruction instruction) {
        try {
            Collection<byte[]> merged = new ArrayList<byte[]>();
            Collection<byte[]> appendValueB = new ArrayList<byte[]>();
            
            for (Map.Entry<byte[], byte[]> entry : results.entrySet()) {
                appendValueB.clear();
                SortedBytesArray.getInstance().parse(entry.getValue()).values(appendValueB);
                this.filter.getReducer().appendRows(merged, entry.getKey(), appendValueB);
            }
            
            if (null == output) output = new ResultToStdout();
            MapperDocuments.deser(merged, output);
            
        } catch (IOException ex) {
            ex.printStackTrace(System.out);
        }
    }
    
    public void execute(String query, Map<String, String> qPart) throws IOException, ParseException {
    	HSearchProcessingInstruction instruction = 
    		new HSearchProcessingInstruction(HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, HSearchProcessingInstruction.OUTPUT_COLS);
        read(query, qPart, instruction , true, true);
    }
}
