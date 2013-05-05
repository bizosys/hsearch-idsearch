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

package com.bizosys.hsearch.idsearch.meta;

import java.io.IOException;

import com.bizosys.hsearch.byteutils.SortedBytesArray;

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
	
	
	private SortedBytesArray parserd = null;
	public static void main(String[] args) throws Exception {
		
	}
	
	public DocumentMeta() {
	}
	
	public DocumentMeta(byte[] data ) throws IOException {
		parserd = SortedBytesArray.getInstanceArr();
		parserd.parse(data);
	}
	
	public DocumentAccess getAccessControl() {
		if ( null != this.accessControl ) return this.accessControl;
		byte[] inputB = this.parserd.getValueAt(SEQUENCE_ACCESS_0);
		if ( inputB.length == 0 ) return null;
		this.accessControl = new DocumentAccess(inputB);
		return this.accessControl;
	}

	public DocumentTypeAndState getFilter() throws IOException {
		if ( null != this.filters ) return this.filters;
		byte[] inputB = this.parserd.getValueAt(SEQUENCE_FILTER_1);
		if ( inputB.length == 0 ) return null;
		this.filters = DocumentTypeAndState.build(inputB);
		return this.filters;
	}

	public DocumentWeight getWeights() throws IOException{
		if ( null != this.weights ) return this.weights;
		byte[] inputB = this.parserd.getValueAt(SEQUENCE_WEIGHTS_2);
		if ( inputB.length == 0 ) return null;
		this.weights = DocumentWeight.build(inputB);
		return this.weights;
	}

	public DocumentTags getTags() throws IOException{
		if ( null != this.tags ) return this.tags;
		byte[] inputB = this.parserd.getValueAt(SEQUENCE_TAGS_3);
		if ( inputB.length == 0 ) return null;
		this.tags = DocumentTags.build(inputB);
		return this.tags;
	}

	public DocumentExtraFields getCustom() throws IOException{
		if ( null != this.custom ) return this.custom;
		byte[] inputB = this.parserd.getValueAt(SEQUENCE_CUSTOM_4);
		if ( inputB.length == 0 ) return null;
		this.custom = DocumentExtraFields.build(inputB);
		return this.custom;
	}
	
	public byte[] toBytes() throws Exception {
		
		SortedBytesArray sba = SortedBytesArray.getInstanceArr();
		
		//SEQUENCE_ACCESS_0;
		byte[] accessControlA = ( null == accessControl) ? (new byte[0]): accessControl.toBytes();

		//SEQUENCE_FILTER_1
		byte[] filtersA = ( null == filters) ? (new byte[0]): filters.toBytes();

		//SEQUENCE_WEIGHTS_2
		byte[] weightsA = ( null == weights) ? (new byte[0]): weights.toBytes();
		
		//SEQUENCE_TAGS_3
		byte[] tagsA = ( null == tags) ? (new byte[0]): tags.toBytes();
		
		//SEQUENCE_CUSTOM_4
		byte[] customA = ( null == custom) ? (new byte[0]): custom.toBytes();
		
		return sba.toBytes(accessControlA, filtersA, weightsA, tagsA, customA);
	}
	
}
