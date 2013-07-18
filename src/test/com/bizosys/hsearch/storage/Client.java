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
