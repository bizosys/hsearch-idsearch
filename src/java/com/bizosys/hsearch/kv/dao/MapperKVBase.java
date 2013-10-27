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

        boolean onRowCols( final int key,  final Object value);
        boolean onRowKey(final int id);
        
        boolean onRowCols(final BitSetWrapper ids, final Object value);
        boolean onRowKey(final BitSetWrapper ids);
        
        void onReadComplete();
    }
}
