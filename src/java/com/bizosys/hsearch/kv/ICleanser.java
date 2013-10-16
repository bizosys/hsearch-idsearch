package com.bizosys.hsearch.kv;

import com.bizosys.hsearch.federate.BitSetOrSet;

public interface ICleanser {
	BitSetOrSet cleanseIds(BitSetOrSet foundIds);
}
