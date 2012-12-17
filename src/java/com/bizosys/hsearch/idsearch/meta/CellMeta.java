package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.ByteArrays;
import com.bizosys.hsearch.byteutils.ByteArrays.ArrayBytes;
import com.google.protobuf.ByteString;

public class CellMeta {

	public DocMetaAccess accessControl;
	public DocMetaFilters filters;
	public DocMetaWeight weights;
	public DocMetaTags tags;
	public DocMetaCustomFields others;

	public static void main(String[] args) throws Exception {
		
	}
	
	public byte[] toBytes() throws Exception {
		ArrayBytes.Builder metaBytes = ByteArrays.ArrayBytes.newBuilder();
		
		if ( null == accessControl) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(accessControl.toBytes()));

		if ( null == filters) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(filters.toBytes()));
		

		if ( null == weights) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(weights.toBytes()));
		
		if ( null == tags) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(tags.toBytes()));
		
		if ( null == others) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(others.toBytes()));
		
		return metaBytes.build().toByteArray();
	}
	
	public DocMetaAccess getAccessControl(byte[] data) throws IOException {
		ByteArrays.ArrayBytes parserd = ByteArrays.ArrayBytes.parseFrom(data);
		byte[] inputB = parserd.getVal(0).toByteArray();
		if ( inputB.length == 0 ) return null;
		return new DocMetaAccess(inputB);
	}
	
	
}
