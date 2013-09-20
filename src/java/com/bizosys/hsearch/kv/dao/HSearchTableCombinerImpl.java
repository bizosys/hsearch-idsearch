package com.bizosys.hsearch.kv.dao;

import java.io.IOException;

import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVBoolean;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVByte;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVDouble;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVFloat;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVIndex;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVInteger;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVLong;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVShort;
import com.bizosys.hsearch.kv.dao.plain.HSearchTableKVString;
import com.bizosys.hsearch.treetable.client.HSearchTableCombiner;
import com.bizosys.hsearch.treetable.client.IHSearchTable;

public final class HSearchTableCombinerImpl extends HSearchTableCombiner {

	@Override
	public final IHSearchTable buildTable(final String tableType) throws IOException {
		
		char thirdLetter = tableType.charAt(2);
		switch ( thirdLetter) {
			case 'I': //KVInteger
				if ( tableType.equals("KVInteger")) return new HSearchTableKVInteger();
				return new HSearchTableKVIndex(); //HSearchTableKVIndex
				
			case 'S': //KVShort KVString
				if ( tableType.equals("KVShort")) return new HSearchTableKVShort();
				return new HSearchTableKVString(); //KVString
			case 'L': //KVLong
				return new HSearchTableKVLong();
			case 'F': //KVFloat
				return new HSearchTableKVFloat();
			case 'D': //KVDouble
				return new HSearchTableKVDouble();
			case 'B': //KVByte KVBoolean
				if ( tableType.equals("KVByte")) return new HSearchTableKVByte();
				return new HSearchTableKVBoolean(); //KVBoolean
			default:
				throw new IOException("Class not found. Missing class HSearchTable" + tableType );
		}
	}
}
