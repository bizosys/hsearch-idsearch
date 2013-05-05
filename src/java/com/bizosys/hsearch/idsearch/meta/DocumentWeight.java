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
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.byteutils.ISortedByte;
import com.bizosys.hsearch.byteutils.SortedBytesFloat;

public class DocumentWeight {
	
	public DocumentWeight(float sourceWeight, float authorWeight,
			float socialClickWeight, float incomingLinkWeight,
			float outgoingLinkWeight, float votingWeight, float editorialWeight) {
		super();
		this.sourceWeight = sourceWeight;
		this.authorWeight = authorWeight;
		this.socialClickWeight = socialClickWeight;
		this.incomingLinkWeight = incomingLinkWeight;
		this.outgoingLinkWeight = outgoingLinkWeight;
		this.votingWeight = votingWeight;
		this.editorialWeight = editorialWeight;
	}

	public float sourceWeight = 0.0f;
	public float authorWeight = 0.0f;
	public float socialClickWeight = 0.0f;
	public float incomingLinkWeight = 0.0f;
	public float outgoingLinkWeight = 0.0f;
	public float votingWeight = 0.0f;
	public float editorialWeight = 0.0f;

	public byte[] toBytes() throws IOException{
		
		return DocumentWeight.toBytes(sourceWeight, authorWeight, 
			socialClickWeight, incomingLinkWeight, 
			outgoingLinkWeight, votingWeight, editorialWeight);

	}
	
	public static byte[] toBytes(float sourceWeight, float authorWeight, float socialClickWeight, float incomingLinkWeight, 
			float outgoingLinkWeight, float votingWeight, float editorialWeight) throws IOException{
		
		ISortedByte<Float> weights = SortedBytesFloat.getInstance();
		List<Float> weightL = new ArrayList<Float>();
		weightL.add(sourceWeight);
		weightL.add(authorWeight);
		weightL.add(socialClickWeight);
		weightL.add(incomingLinkWeight);
		weightL.add(outgoingLinkWeight);
		weightL.add(votingWeight);
		weightL.add(editorialWeight);
		
		return weights.toBytes(weightL);

	}

	public static DocumentWeight build(byte[] data) throws IOException {
		ISortedByte<Float> weights = SortedBytesFloat.getInstance().parse(data);
		List<Float> weightL = (List<Float>) weights.values();
		
		return new DocumentWeight(
			weightL.get(0).floatValue(), weightL.get(1).floatValue(), weightL.get(2).floatValue(),
			weightL.get(3).floatValue(),weightL.get(4).floatValue(),weightL.get(5).floatValue(),weightL.get(6).floatValue()
		);
	}
	
	public static void main(String[] args) throws IOException {
		DocumentWeight serWeight = new DocumentWeight(1f,2f,3f,4f,5f,6f,7f);
		byte[] ser = serWeight.toBytes();
		
		long start = System.currentTimeMillis();
		DocumentWeight dm = null;
		for ( int i=0; i<1000000; i++) {
			dm = DocumentWeight.build(ser);
		}
		long end = System.currentTimeMillis();
		System.out.println ( dm.editorialWeight + "   in " + (end - start) );
	}
}
