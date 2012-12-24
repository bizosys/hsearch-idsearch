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
package com.bizosys.hsearch.index;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.bizosys.hsearch.index.util.ContentField;
import com.bizosys.hsearch.index.util.ContentFieldReader;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;
import com.oneline.util.StringUtils;

/**
 * This is an abstract class which reads the various dimensions of 
 * the document and tokenizes them including ID, URL, Fields, Title. 
 * @author karan
 *
 */
public final class TokenizeBase {
	
	private static final boolean DEBUG_ENABLED = InpipeLog.l.isDebugEnabled();

	public static final char TAG_SEPARATOR_STORED = '\t';
	public static final String TAG_SEPARATOR_STORED_S = "\t";

	public static final char TAG_SEPARATOR_SHOWN = ',';
	public static final char[] URL_SEPARATOR = new char[]{'-','_','/','.','?','&','=',':'};
	public static final char[] FIELDVAL_SEPARATOR = new char[]{',','/','=',':'};
	
	
	/**
	 * Pack different sections with different readers.
	 * This potentially helps on weight assignment.
	 * @param aDoc	A document
	 * @return	Reader types
	 */
	public static List<ContentFieldReader> getReaders(List<ContentField> contentFields) throws SystemFault, ApplicationFault {
		
		List<ContentFieldReader> readers = new ArrayList<ContentFieldReader>();
		for (ContentField aField : contentFields) {
			ContentFieldReader reader = createReader(aField);
			if ( null != reader) readers.add(reader);
		}
		return readers;
	}

	private static ContentFieldReader createReader(ContentField aField) throws SystemFault, ApplicationFault {
		
		//String text = aField.content.toLowerCase();
		String text = aField.content;
		
		text = StringUtils.replaceMultipleCharsToAnotherChar(text, FIELDVAL_SEPARATOR, ' ');		
		if (aField.searchable) {
			InputStream ba = new ByteArrayInputStream( aField.content.getBytes());
			InputStreamReader is = new InputStreamReader(ba);
			return new ContentFieldReader(aField.name,aField.weight, is);
		}
		return null;
	}
}
