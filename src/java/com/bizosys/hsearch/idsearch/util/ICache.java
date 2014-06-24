/*
* Copyright 2010 Bizosys Technologies Limited
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
package com.bizosys.hsearch.idsearch.util;

import com.bizosys.hsearch.util.HSearchConfig;

public abstract class ICache {
	
	public static ICache instance = null;
	public static ICache getInstance() {
		if ( null != instance) return instance;
		synchronized (ICache.class.getName()) {
			if ( null != instance) return instance;
			int lruCacheMb = HSearchConfig.getInstance().getConfiguration().getInt("cache.size.mb", 1024);
			int cacheRefreshDurationInSecs = HSearchConfig.getInstance().getConfiguration().getInt("cache.refresh.secs", -1);
			instance = new LruCache();
			instance.set(lruCacheMb, cacheRefreshDurationInSecs);
		}
		return instance;
	}
	
	public abstract byte[] get(String key);
	public abstract Object getPinned(String key);
	public abstract void clear();
	public abstract boolean containsKey(String key);
	public abstract boolean containsPinnedKey(String key);
	public abstract void put(String key, byte[] val);
	public abstract void putPinned(String key, Object val);
    public abstract void set(int lruCacheMb, int cacheRefreshDurationInSecs);
	
}
