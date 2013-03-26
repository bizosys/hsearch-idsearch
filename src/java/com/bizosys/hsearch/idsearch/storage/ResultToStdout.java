package com.bizosys.hsearch.idsearch.storage;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class ResultToStdout implements Map<String, Float> {

	@Override
	public void clear() {
	}

	@Override
	public boolean containsKey(Object key) {
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		return false;
	}

	@Override
	public Set<java.util.Map.Entry<String, Float>> entrySet() {
		return null;
	}

	@Override
	public Float get(Object key) {
		return null;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public Set<String> keySet() {
		return null;
	}

	@Override
	public Float put(String key, Float value) {
		System.out.println(key + "-" + value.floatValue());
		return value;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Float> m) {
	}

	@Override
	public Float remove(Object key) {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public Collection<Float> values() {
		return null;
	}
}
