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

package com.bizosys.hsearch.kv.dao;

import com.bizosys.hsearch.federate.BitSetWrapper;
import com.bizosys.hsearch.treetable.client.IHSearchPlugin;

public abstract class MapperKVBase implements IHSearchPlugin {
    
    public abstract TablePartsCallback getPart();
	
    public interface TablePartsCallback {
    	
    	/**
    	 * This is called when the hsearch instruction output type is 
    	 * HSearchProcessingInstruction.OUTPUT_COLS and the repeatable property
    	 * in schema is false.
    	 * @param key
    	 * @param value
    	 * @return
    	 */
        boolean onRowCols( final int key,  final Object value);

    	/**
    	 * This is called when the hsearch instruction output type is 
    	 * HSearchProcessingInstruction.OUTPUT_ID and the repeatable property
    	 * in schema is false.
    	 * @param key
    	 * @param value
    	 * @return
    	 */
        boolean onRowKey(final int id);

    	/**
    	 * This is called when the hsearch instruction output type is 
    	 * HSearchProcessingInstruction.OUTPUT_COLS and the repeatable property
    	 * in schema is true.
    	 * @param key
    	 * @param value
    	 * @return
    	 */
        boolean onRowCols(final BitSetWrapper ids, final Object value);

    	/**
    	 * This is called when the hsearch instruction output type is 
    	 * HSearchProcessingInstruction.OUTPUT_ID and the repeatable property
    	 * in schema is true.
    	 * @param key
    	 * @param value
    	 * @return
    	 */
        boolean onRowKey(final BitSetWrapper ids);
        
        /**
         * This method is called when all the rows has been processed.
         */
        void onReadComplete();
    }
}
