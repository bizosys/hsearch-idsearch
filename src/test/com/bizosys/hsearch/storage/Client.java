package com.bizosys.hsearch.storage;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.storage.donotmodify.HBaseTableSchema;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.treetable.storage.HSearchGenericFilter;
import com.bizosys.hsearch.treetable.storage.HSearchTableReader;

public class Client extends HSearchTableReader {

    HSearchGenericFilter filter = null;

    public Client() throws IOException {
        HBaseTableSchema.getInstance();
    }

    @Override
    public HSearchGenericFilter getFilter(String multiQuery,
            Map<String, String> multiQueryParts, HSearchProcessingInstruction outputType) {
        filter = new Filter(outputType, multiQuery, multiQueryParts);
        return filter;
    }

    final byte[]  blankkey = "".getBytes();
    @Override
    public void rows(Collection<byte[]> results, HSearchProcessingInstruction instruction) {

        try {

            Collection<byte[]> merged = new ArrayList<byte[]>();
            Collection<byte[]> appendValueB = new ArrayList<byte[]>();

            for (byte[] data : results) {
                appendValueB.clear();
                SortedBytesArray.getInstance().parse(data).values(appendValueB);
                this.filter.getReducer().appendRows(merged, blankkey, appendValueB);
            }

            if (merged.iterator().hasNext()) {
                //Deserialize and read the output
            }
        } catch (IOException ex) {
            ex.printStackTrace(System.out);
        }

    }

    public void execute(String tableName, String query, Map<String, String> qPart) throws IOException, ParseException {
    	HSearchProcessingInstruction instruction = 
    		new HSearchProcessingInstruction(HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS, HSearchProcessingInstruction.OUTPUT_COLS);
        read(tableName, query, qPart, instruction , true, true);
    }
}
