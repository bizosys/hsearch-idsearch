package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.ByteArrays;
import com.bizosys.hsearch.byteutils.ByteArrays.ArrayBytes;
import com.google.protobuf.ByteString;

public class DocumentMeta {

	public DocumentAccess accessControl;
	public DocumentTypeAndState filters;
	public DocumentWeight weights;
	public DocumentTags tags;
	public DocumentExtraFields custom;

	private static final int SEQUENCE_ACCESS_0 = 0;
	private static final int SEQUENCE_FILTER_1 = 1;
	private static final int SEQUENCE_WEIGHTS_2 = 2;
	private static final int SEQUENCE_TAGS_3 = 3;
	private static final int SEQUENCE_CUSTOM_4 = 4;
	
	
	private ByteArrays.ArrayBytes parserd = null;
	public static void main(String[] args) throws Exception {
		
	}
	
	public DocumentMeta() {
	}
	
	public DocumentMeta(byte[] data ) throws IOException {
		this.parserd = ByteArrays.ArrayBytes.parseFrom(data);
	}
	
	public DocumentAccess getAccessControl() {
		if ( null != this.accessControl ) return this.accessControl;
		byte[] inputB = this.parserd.getVal(SEQUENCE_ACCESS_0).toByteArray();
		if ( inputB.length == 0 ) return null;
		this.accessControl = new DocumentAccess(inputB);
		return this.accessControl;
	}

	public DocumentTypeAndState getFilter() throws IOException {
		if ( null != this.filters ) return this.filters;
		byte[] inputB = this.parserd.getVal(SEQUENCE_FILTER_1).toByteArray();
		if ( inputB.length == 0 ) return null;
		this.filters = DocumentTypeAndState.build(inputB);
		return this.filters;
	}

	public DocumentWeight getWeights() throws IOException{
		if ( null != this.weights ) return this.weights;
		byte[] inputB = this.parserd.getVal(SEQUENCE_WEIGHTS_2).toByteArray();
		if ( inputB.length == 0 ) return null;
		this.weights = DocumentWeight.build(inputB);
		return this.weights;
	}

	public DocumentTags getTags() throws IOException{
		if ( null != this.tags ) return this.tags;
		byte[] inputB = this.parserd.getVal(SEQUENCE_TAGS_3).toByteArray();
		if ( inputB.length == 0 ) return null;
		this.tags = DocumentTags.build(inputB);
		return this.tags;
	}

	public DocumentExtraFields getCustom() throws IOException{
		if ( null != this.custom ) return this.custom;
		byte[] inputB = this.parserd.getVal(SEQUENCE_CUSTOM_4).toByteArray();
		if ( inputB.length == 0 ) return null;
		this.custom = DocumentExtraFields.build(inputB);
		return this.custom;
	}
	
	public byte[] toBytes() throws Exception {
		ArrayBytes.Builder metaBytes = ByteArrays.ArrayBytes.newBuilder();
		
		//SEQUENCE_ACCESS_0;
		if ( null == accessControl) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(accessControl.toBytes()));

		//SEQUENCE_FILTER_1
		if ( null == filters) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(filters.toBytes()));
		
		//SEQUENCE_WEIGHTS_2
		if ( null == weights) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(weights.toBytes()));
		
		//SEQUENCE_TAGS_3
		if ( null == tags) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(tags.toBytes()));
		
		//SEQUENCE_CUSTOM_4
		if ( null == custom) metaBytes.addVal(ByteString.copyFrom(new byte[0]) );
		metaBytes.addVal(ByteString.copyFrom(custom.toBytes()));
		
		return metaBytes.build().toByteArray();
	}
	
}
