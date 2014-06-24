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
package com.bizosys.hsearch.idsearch.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
 
public class LruCache extends ICache{
	
 
	
	Map<String, byte[]> syncedLruMap = null;
	Map<String, Object> syncedPinnedMap = null;

	private long allowedSizeInBytes; // Maximum number of items in the cache.
	private int allowedPinnedObjects = 100; // Maximum number of items in the cache.

    private Timer cacheRefreshTimer = null;
    private CacheRefreshAgent cacheRefreshAgent = null;

    public LruCache() {
    	
    }
    
    @Override
    public void set(int lruCacheMb, int cacheRefreshDurationInSecs) {
    	
		syncedLruMap = Collections.synchronizedMap(new LruMap());
		syncedPinnedMap = Collections.synchronizedMap(
				new LinkedHashMap<String, Object>(96 , 1.5f, true) );
		
        this.allowedSizeInBytes = lruCacheMb * 1024 * 1024;
        long useForCache = Runtime.getRuntime().totalMemory() /2 ; 
        if ( useForCache < this.allowedSizeInBytes ) {
        	IdSearchLog.l.info("Adjusting cache memory to " + useForCache/1024/1024 + " (mb)");
        	this.allowedSizeInBytes = useForCache;
        }
        
        if ( cacheRefreshDurationInSecs > 0 ) {
        	cacheRefreshTimer = new Timer(true);
        	this.cacheRefreshAgent = new CacheRefreshAgent();
        	cacheRefreshTimer.schedule(this.cacheRefreshAgent, 
        		cacheRefreshDurationInSecs * 1000 , cacheRefreshDurationInSecs * 1000);
        }
	}
	
	@Override
    public byte[] get(String key) {
    	return this.syncedLruMap.get(key);
    }

	@Override
    public Object getPinned(String key) {
    	return this.syncedPinnedMap.get(key);
    }

	@Override
    public void clear() {
    	this.syncedLruMap.clear();
    	this.syncedPinnedMap.clear();
    }
    
	@Override
    public boolean containsKey(String key) {
    	return this.syncedLruMap.containsKey(key);
    }
        
	@Override
    public boolean containsPinnedKey(String key) {
    	return this.syncedPinnedMap.containsKey(key);
    }

	@Override
    public final void put(final String key, final byte[] val) {
    	
    	long newObjSize = ( null == val) ? 0 : val.length;
    	long currentSize = new Double( Float.intBitsToFloat(syncedLruMap.size()) * 1024 * 1024).longValue();
    	long requiredSize =  currentSize + newObjSize;
    	
		List<String> deleteIds = null;
    	if ( requiredSize > allowedSizeInBytes) {
    		deleteIds = new ArrayList<String>();
        	for (Map.Entry<String, byte[]> entry : syncedLruMap.entrySet()) {
            	long entrySize = ( null == entry.getValue()) ? 0 : entry.getValue().length;
            	requiredSize = requiredSize - entrySize;
            	deleteIds.add(entry.getKey());
            	if ( requiredSize <= allowedSizeInBytes) break;
    		}
    	}
    	
    	if ( null != deleteIds) {
        	for (String delId : deleteIds) {
        		syncedLruMap.remove(delId);
    		}
        	IdSearchLog.l.info("Cache Eviction: " + deleteIds.toString());
    	}
    	syncedLruMap.put(key, val);
    }

	@Override
    public final void putPinned(final String key, final Object val) {
		String deleteId = null;
    	if ( this.syncedPinnedMap.size() > allowedPinnedObjects) {
        	for (Map.Entry<String, Object> entry : syncedPinnedMap.entrySet()) {
            	deleteId = entry.getKey();
            	break;
    		}
    	}
    	
    	if ( null != deleteId) {
    		syncedPinnedMap.remove(deleteId);
        	IdSearchLog.l.info("Pinned Cache Eviction: " + deleteId.toString());
    	}
    	syncedPinnedMap.put(key, val);
    }
    
    @Override
    public String toString() {
		return syncedLruMap.toString();
    }
    
	public class LruMap extends LinkedHashMap<String, byte[]> {

		private static final long serialVersionUID = 1L;
		private long currentSizeInBytes; // Maximum number of items in the cache.

		private LruMap() { 
	        super(1024, 1.5f, true); // Pass 'true' for accessOrder.
	    }
	    
	    @Override
	    public final byte[] put(final String key, final byte[] val) {
	    	if ( containsKey(key) ) {
	    		byte[] existingB = get(key);
	    		long existingSize = ( null == existingB) ? 0 : existingB.length;
	    		currentSizeInBytes = currentSizeInBytes - existingSize;
	    	} 
	    	
	    	byte[] res = super.put(key, val);
			long newSize = ( null == val) ? 0 : val.length;
			currentSizeInBytes = currentSizeInBytes + newSize;
	    	return res;
	    }
	    
		@Override
		public final byte[] remove(final Object key) {
			Object valO = this.get(key);
	    	long objSize = ( null == valO) ? 0 : ((byte[]) valO).length;
	    	currentSizeInBytes = currentSizeInBytes - objSize;
			return super.remove(key);
		}

		@Override
		public final void clear() {
			super.clear();
	    	currentSizeInBytes = 0;
		}

		@Override
		public final int size() {
			float fillSizeInMb = currentSizeInBytes;
			fillSizeInMb = fillSizeInMb/1024/1024;
			return Float.floatToIntBits(fillSizeInMb);
		}
		
	    @Override
	    public String toString() {
	    	StringBuilder sb = new StringBuilder();
			float fillSizeInMb = currentSizeInBytes;
			fillSizeInMb = fillSizeInMb/1024/1024;
			sb.append("Used Size (MB):	" + fillSizeInMb).append('\n');
			sb.append("Total Elements:	" + super.size()).append('\n');
	    	for (Map.Entry<String, byte[]> key : this.entrySet()) {
	    		byte[] val = key.getValue();
	    		sb.append(key.getKey()).append('\t').append( (null == val) ? 0 : val.length).append('\n');
			}
			return sb.toString();
	    }
		
	}
	
	
	
    //--------------Cache Check Module-------------------
    public synchronized void cacheCheck() {
    	if ( IdSearchLog.l.isDebugEnabled()) 
    	{
    		IdSearchLog.l.debug("Starting cache clear. Cache Status\n" + this.toString());
    	}
    	/**
    	 * We can clear the cache.. However, this we should do more intelligently along with warming. For now, let's just clear. 
    	 */
    	this.clear();
    }
    
    public synchronized void stop() 
    {
    	IdSearchLog.l.debug("Stoping the cache check service");
    	if ( null != this.cacheRefreshAgent ) 
    	{
    		this.cacheRefreshAgent.cancel();
    		this.cacheRefreshAgent = null;
    	}
    }
    
    
    private final class CacheRefreshAgent extends TimerTask {

		public void run() {
            try {
            	cacheCheck();
            } catch(Exception e) {
    			IdSearchLog.l.fatal("Error in running cache check", e);
            }
        }
    }    
    
    public static void main(String[] args) throws InterruptedException {
    	ICache cache = ICache.getInstance();
    	
    	cache.put("a1", new byte[1024*100]);
    	cache.put("a2", new byte[1024*100]);
    	cache.put("a3", new byte[1024*100]);
    	
    	cache.get("a2");
    	cache.get("a2");
    	cache.get("a2");
    	cache.get("a1");
    	
    	for ( int i=0; i< 140; i++) {
        	cache.put("Key " + i, new byte[1024*100]);
        	cache.get("a2");
    	}
    	System.out.println("STATE:\n" + cache.toString());
    	cache.clear();
    	System.out.println("CLEARED:" + cache.toString());
    	
    	for ( int i=0; i<5000; i++) {
        	cache.putPinned("A" + i, new Integer(1));
    	}
    	System.out.println( cache.getPinned("A4999") );
    	
    }
}