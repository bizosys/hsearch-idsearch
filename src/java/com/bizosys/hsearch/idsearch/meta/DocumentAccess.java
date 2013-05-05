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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellComparator;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class DocumentAccess {
	
	Cell2<Integer, Integer> accessCell = null;
	
	public DocumentAccess() {
		accessCell = new Cell2<Integer, Integer>(SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance());
	}
	

	public DocumentAccess(byte[] data) {
		accessCell = new Cell2<Integer, Integer>(SortedBytesInteger.getInstance(), SortedBytesInteger.getInstance(), data);
	}
	
	public void setAccess(Map<Integer, Integer> typesAndValues) throws IOException{
		
		for (Integer accessTypeCode: typesAndValues.keySet()) {
			accessCell.add(accessTypeCode, typesAndValues.get(accessTypeCode));
		}
	}

	public void setAccess(int accessTypeSuchAsRole, int accessValueSuchAsArchitect) throws IOException{
		accessCell.add(accessTypeSuchAsRole, accessValueSuchAsArchitect);
	}
	
	public List<CellKeyValue<Integer, Integer>> getAccesses() throws IOException {
		return accessCell.getMap();
	}

	public Set<Integer> getAccessTypes(Integer byAccessValueSuchAsArchitect) throws IOException {
		return accessCell.keySet(byAccessValueSuchAsArchitect);
	}

	public void getAccessTypes(Integer byAccessValueSuchAsArchitect, Set<Integer> foundAccessTypes) throws IOException {
		accessCell.keySet(byAccessValueSuchAsArchitect, foundAccessTypes);
	}

	public boolean hasAccess(Map<Integer, Integer> userAccessTypeAndValues) throws IOException {
		Set<Integer> tempMatchingAccessTypes = new HashSet<Integer>();
		return hasAccess(userAccessTypeAndValues, tempMatchingAccessTypes);
	}
	
	/**
	 * 1 - Go through each user access profile values and find if any matching keys in the document access.
	 * 
	 * @param userAccessTypeAndValues
	 * @param tempMatchingAccessTypes
	 * @return
	 * @throws IOException
	 */
	public boolean hasAccess(Map<Integer, Integer> userAccessTypeAndValues, Set<Integer> tempMatchingAccessTypes) throws IOException {
		
		for (Integer aProfileValue : userAccessTypeAndValues.values()) {
			tempMatchingAccessTypes.clear();
					
			accessCell.keySet(aProfileValue, tempMatchingAccessTypes); 

			if ( tempMatchingAccessTypes.size() == 0 ) continue;

			//For the user profile value (such as architect role), is the document access authoring.
			for (Integer userProfileType : userAccessTypeAndValues.keySet()) {
				if ( tempMatchingAccessTypes.contains(userProfileType)) return true;
			}
		}
		return false;
	}
	
	public byte[] toBytes() throws IOException{
		accessCell.sort(new CellComparator.IntegerComparator<Integer>());
		return accessCell.toBytesOnSortedData();
	}
}
