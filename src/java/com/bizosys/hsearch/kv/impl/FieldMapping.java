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

	public static class Field {

		public String name;
		public  String sourceName;
		public  int sourceSeq;
		public  boolean isIndexable;
		public  boolean isSave;
		public  boolean isRepeatable;
		public  boolean isMergedKey;
		public  int mergePosition;
		public  boolean isJoinKey;
		public boolean skipNull;
		public String defaultValue;
		public  String fieldType;
		public  String analyzer;

		public boolean isDocIndex;
		
		public Field() {
		}

		public Field(String name,String sourceName, int sourceSeq, boolean isIndexable,boolean isSave,
				boolean isRepeatable, boolean isMergedKey, int mergePosition,
				boolean isJoinKey, boolean skipNull, String defaultValue, String fieldType, String analyzer, boolean isDocIndex) {
			
			this.name = name;
			this.sourceName = sourceName;
			this.sourceSeq = sourceSeq;
			this.isIndexable = isIndexable;
			this.isSave = isSave;
			this.isRepeatable = isRepeatable;
			this.isMergedKey = isMergedKey;
			this.mergePosition = mergePosition;
			this.isJoinKey = isJoinKey;
			this.skipNull = skipNull;
			this.defaultValue = defaultValue;
			this.fieldType = fieldType;
			this.analyzer = analyzer;
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

	private Field field;
	
	public Map<Integer, Field> fieldSeqs = new HashMap<Integer, Field>();
	public Map<String, Field> nameSeqs = new HashMap<String, Field>();
	public String tableName = null;
	public String familyName = null;
	public char fieldSeparator = '|';
	
	String name = null;
	String sourceName = null;
	int sourceSeq = 0;
	boolean isIndexable = false;
	boolean isSave = false;
	boolean isRepeatable = false;
	boolean isMergedKey = false;
	int mergePosition = 0;
	boolean isJoinKey = false;
	boolean skipNull = false;
	String defaultValue = null;;	
	String fieldType = null;;
	String analyzer = null;
	boolean isDocIndex = false;
	
	public static FieldMapping getInstance(){
		return new FieldMapping();
	}
	
	private FieldMapping() {

	}

	public static void main(String[] args) throws IOException, SAXException,
			ParserConfigurationException {
		
		String xmlString = "<schema name='call-record'><fields><field name='2' sourcesequence='166' sourcename='FILEID' type='int' indexed='true' isSave='true' repeatable='true' mergekey='true' mergeposition='0' isjoinkey='false' skipNull='true' defaultValue=''/><field name='677' sourcesequence='142' sourcename='CUSTOMFIELD5' type='String' indexed='true' isSave='true' repeatable='true' mergekey='false' mergeposition='' isjoinkey='false' skipNull='true' defaultValue=''/></fields></schema>";
		FieldMapping fm = getXMLStringFieldMappings(xmlString);
		fm.display();

	}
	
	public static final FieldMapping getXMLStringFieldMappings(final String xmlString){
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser;
		FieldMapping fieldMapping = null;
		try {

			saxParser = saxFactory.newSAXParser();
			fieldMapping = new FieldMapping();
			saxParser.parse(new InputSource(new StringReader(xmlString)), fieldMapping);
			
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		}
		
		return fieldMapping;
	}
	
	public static final FieldMapping getXMLFieldMappings(final String filePath) throws ParseException {

		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser;
		FieldMapping fieldMapping = null;
		try {

			saxParser = saxFactory.newSAXParser();
			fieldMapping = new FieldMapping();
			File file = new File(filePath);
			saxParser.parse(file, fieldMapping);
			return fieldMapping;
			
		} catch (ParserConfigurationException e) {
			HSearchLog.l.fatal("File Path:" + filePath , e);
			e.printStackTrace(System.err);
			throw new ParseException(e.getMessage(), 0);
		} catch (SAXException e) {
			HSearchLog.l.fatal("File Path:" + filePath , e);
			e.printStackTrace(System.err);
			throw new ParseException(e.getMessage(), 0);
		}catch (IOException e) {
			HSearchLog.l.fatal("File Path:" + filePath , e);
			e.printStackTrace(System.err);
			throw new ParseException(e.getMessage(), 0);
		}
	}

	public void startElement(String uri, String localName, String qName,
			Attributes attributes) throws SAXException {

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
			isSave = attributes.getValue("isSave").equalsIgnoreCase("true") ? true : false;
			isRepeatable = attributes.getValue("repeatable").equalsIgnoreCase("true") ? true : false;
			isMergedKey = attributes.getValue("mergekey").equalsIgnoreCase("true") ? true : false;
			mergePosition = (null == attributes.getValue("mergeposition") || (attributes.getValue("mergeposition").length() == 0)) 
								? -1 : Integer.parseInt(attributes.getValue("mergeposition"));
			isJoinKey = attributes.getValue("isjoinkey").equalsIgnoreCase("true") ? true : false;

			skipNull = attributes.getValue("skipNull").equalsIgnoreCase("true") ? true : false;
			defaultValue = attributes.getValue("defaultValue");
			analyzer = attributes.getValue("analyzer");
			isDocIndex = (null == analyzer) ? false : analyzer.length() > 0;
			
			field = new Field(name, sourceName, sourceSeq, isIndexable, isSave, isRepeatable,
					isMergedKey, mergePosition, isJoinKey, skipNull, defaultValue, fieldType, analyzer, isDocIndex);
		}
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equalsIgnoreCase("field")) {
			fieldSeqs.put(sourceSeq, field);
			nameSeqs.put(name, field);
		}
	}

	private void display() {
		for (Map.Entry<Integer, Field> entry : fieldSeqs.entrySet()) {
			System.out.println(entry.getKey() + " - "+ entry.getValue().toString());
		}
		if ( DEBUG_ENABLED )  HSearchLog.l.debug("sahema name is " + tableName);
	}
}
