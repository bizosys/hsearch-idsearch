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

package com.bizosys.hsearch.kv.impl;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.bizosys.hsearch.util.HSearchLog;

public class FieldMapping extends DefaultHandler {

	private static final boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();

	public class Field {

		public String name;
		public  String sourceName;
		public  int sourceSeq;
		public  boolean isIndexable;
		public  boolean isStored;
		public  boolean isRepeatable;
		public  boolean isMergedKey;
		public  int mergePosition;
		public  boolean isJoinKey;
		public boolean skipNull;
		public String defaultValue;
		public String fieldType;
		public String analyzer;
		public boolean isAnalyzed;

		public boolean isDocIndex;
		
		public Field() {
		}

		public Field(String name,String sourceName, int sourceSeq, boolean isIndexable,boolean isStored,
				boolean isRepeatable, boolean isMergedKey, int mergePosition,
				boolean isJoinKey, boolean skipNull, String defaultValue, String fieldType, String analyzer, 
				boolean isDocIndex, boolean isAnalyzed) {
			
			this.name = name;
			this.sourceName = sourceName;
			this.sourceSeq = sourceSeq;
			this.isIndexable = isIndexable;
			this.isStored = isStored;
			this.isRepeatable = isRepeatable;
			this.isMergedKey = isMergedKey;
			this.mergePosition = mergePosition;
			this.isJoinKey = isJoinKey;
			this.skipNull = skipNull;
			this.defaultValue = defaultValue;
			this.fieldType = fieldType;
			this.analyzer = analyzer;
			this.isAnalyzed = isAnalyzed;
			this.isDocIndex = isDocIndex;
		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(sourceName).append('\t').append(name).append('\t').append(sourceSeq).append('\t')
					.append(isIndexable).append('\t').append(isRepeatable)
					.append('\t').append(isMergedKey).append('\t')
					.append(skipNull).append('\t').append(defaultValue)
					.append(isJoinKey).append('\t').append(fieldType).append('\t').append(analyzer);

			return sb.toString();
		}
	}

	public Field field;
	
	public Map<Integer, Field> fieldSeqs = new HashMap<Integer, Field>();
	public Map<String, Field> nameSeqs = new HashMap<String, Field>();
	public String tableName = null;
	public String familyName = null;
	public char fieldSeparator = '|';
	
	public String name = null;
	public String sourceName = null;
	public int sourceSeq = 0;
	public boolean isIndexable = false;
	public boolean isStored = false;
	public boolean isRepeatable = false;
	public boolean isMergedKey = false;
	public int mergePosition = 0;
	public boolean isJoinKey = false;
	public boolean skipNull = false;
	public String defaultValue = null;;	
	public String fieldType = null;;
	public String analyzer = null;
	public boolean isAnalyzed = false;
	public boolean isDocIndex = false;
	
	public static FieldMapping getInstance(){
		return new FieldMapping();
	}
	
	public FieldMapping() {

	}

	public static void main(String[] args) throws IOException, SAXException,
			ParserConfigurationException, ParseException {
		
		String xmlString = "<schema tableName=\"findings-detail\" familyName=\"1\" fieldSeparator=\"|\"><fields><field name=\"2\" sourcesequence=\"166\" sourcename=\"FILEID\" type=\"int\" indexed=\"true\" stored=\"true\" analyzer=\"\" analyzed=\"false\" repeatable=\"true\" mergekey=\"true\" mergeposition=\"0\" isjoinkey=\"false\" skipNull=\"false\" defaultValue=\"-2147483648\" /><field name=\"524\" sourcesequence=\"99\" sourcename=\"PCDID\" type=\"int\" indexed=\"true\" stored=\"true\" analyzer=\"\" analyzed=\"false\" repeatable=\"true\" mergekey=\"true\" mergeposition=\"1\" isjoinkey=\"false\" skipNull=\"false\" defaultValue=\"-2147483648\"/></fields></schema>";
		FieldMapping fm = FieldMapping.getInstance();
		fm.parseXMLString(xmlString);
		fm.display();

	}
	
	public void parseXMLString(final String xmlString) throws ParseException{
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser;
		try {

			saxParser = saxFactory.newSAXParser();
			saxParser.parse(new InputSource(new StringReader(xmlString)), this);
			
		} catch (Exception e) {
			HSearchLog.l.fatal("File Path: ", e);
			throw new ParseException(e.getMessage(), 0);
		}
	}
	
	public void parseXML(final String filePath) throws ParseException {

		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser;
		try {

			saxParser = saxFactory.newSAXParser();
			File file = new File(filePath);
			saxParser.parse(file, this);
			
		} catch (Exception e) {
			HSearchLog.l.fatal("File Path:" + filePath , e);
			throw new ParseException(e.getMessage(), 0);
		}
	}

	public void startElement(String uri, String localName, String qName,Attributes attributes) throws SAXException {

		if(qName.equalsIgnoreCase("schema")){
			tableName = attributes.getValue("tableName");
			tableName = null == tableName ? "kv-store" : (tableName.length() == 0 ? "kv-store" : tableName);		
			familyName = attributes.getValue("familyName");
			familyName = null == familyName ? "1" : (familyName.length() == 0 ? "1" : familyName);
			String separator = attributes.getValue("fieldSeparator");
			separator = null == separator ? "|" : (separator.length() == 0 ? "|" : separator);
			fieldSeparator = separator.charAt(0);
		}
		if (qName.equalsIgnoreCase("field")) {

			name = attributes.getValue("name");
			sourceName = attributes.getValue("sourcename");
			sourceSeq = (null == attributes.getValue("sourcesequence") || (attributes
					.getValue("sourcesequence").length() == 0)) ? -1 : Integer
					.parseInt(attributes.getValue("sourcesequence"));
			fieldType = attributes.getValue("type");
			isIndexable = attributes.getValue("indexed").equalsIgnoreCase("true") ? true : false;
			isStored = attributes.getValue("stored").equalsIgnoreCase("true") ? true : false;
			isRepeatable = attributes.getValue("repeatable").equalsIgnoreCase("true") ? true : false;
			isMergedKey = attributes.getValue("mergekey").equalsIgnoreCase("true") ? true : false;
			mergePosition = (null == attributes.getValue("mergeposition") || (attributes.getValue("mergeposition").length() == 0)) 
								? -1 : Integer.parseInt(attributes.getValue("mergeposition"));
			isJoinKey = attributes.getValue("isjoinkey").equalsIgnoreCase("true") ? true : false;

			skipNull = attributes.getValue("skipNull").equalsIgnoreCase("true") ? true : false;
			defaultValue = attributes.getValue("defaultValue");
			analyzer = attributes.getValue("analyzer");
			isAnalyzed = attributes.getValue("analyzed").equalsIgnoreCase("true") ? true : false;
			isDocIndex = (null == analyzer) ? false : analyzer.length() > 0;
			
			field = new Field(name, sourceName, sourceSeq, isIndexable, isStored, isRepeatable,
					isMergedKey, mergePosition, isJoinKey, skipNull, defaultValue, fieldType, analyzer, 
					isDocIndex, isAnalyzed);
		}
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equalsIgnoreCase("field")) {
			fieldSeqs.put(sourceSeq, field);
			nameSeqs.put(name, field);
		}
	}

	public void display() {
		for (Map.Entry<Integer, Field> entry : fieldSeqs.entrySet()) {
			System.out.println(entry.getKey() + " - "+ entry.getValue().toString());
		}
		if ( DEBUG_ENABLED )  HSearchLog.l.debug("sahema name is " + tableName);
	}
}
