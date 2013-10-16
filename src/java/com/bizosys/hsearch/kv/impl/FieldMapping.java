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
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.bizosys.hsearch.util.HSearchLog;
import com.bizosys.unstructured.util.IdSearchLog;

public class FieldMapping extends DefaultHandler {

	private static final boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();

	public class Field implements Comparable<Field>{

		public String name;
		public  String sourceName;
		public  int sourceSeq;
		public  boolean isIndexable;
		public  boolean isStored;
		public  boolean isRepeatable;
		public  boolean isMergedKey;
		public  int mergePosition;
		public boolean skipNull;
		public String defaultValue;
		private String dataType;
		public String analyzer;
		public boolean isAnalyzed;
		public boolean isCompressed;

		public boolean keepPhrase;
		
		public boolean isDocIndex;

		public boolean isSeparable = false;

		public boolean isCachable = true;
		
		public Field() {
		}
		
		public String getDataType() {
			return dataType;
//			if ( "Text".equals(dataType) ) return "String";
//			else return dataType;
		}

		public Field(String name,String sourceName, int sourceSeq, boolean isIndexable,boolean isStored,
				boolean isRepeatable, boolean isMergedKey, int mergePosition,
				boolean skipNull, String defaultValue, String fieldType, String analyzer, 
				boolean isDocIndex, boolean isAnalyzed, boolean isSeparable, boolean isCachable,
				boolean isCompressed, boolean keepPhrase) {
			
			this.name = name;
			this.sourceName = sourceName;
			this.sourceSeq = sourceSeq;
			this.isIndexable = isIndexable;
			this.isStored = isStored;
			this.isRepeatable = isRepeatable;
			this.isMergedKey = isMergedKey;
			this.mergePosition = mergePosition;
			this.skipNull = skipNull;
			this.defaultValue = defaultValue;
			this.dataType = fieldType;
			this.analyzer = analyzer;
			this.isAnalyzed = isAnalyzed;
			this.isDocIndex = isDocIndex;
			
			this.isSeparable = isSeparable;
			this.isCachable = isCachable;
			this.isCompressed = isCompressed;
			this.keepPhrase = keepPhrase;
		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(sourceName).append('\t').append(name).append('\t').append(sourceSeq).append('\t')
					.append(isIndexable).append('\t').append(isRepeatable)
					.append('\t').append(isMergedKey).append('\t')
					.append(skipNull).append('\t').append(defaultValue)
					.append('\t').append(dataType).append('\t').append(analyzer)
					.append('\t').append(isCachable).append('\t').append(isCompressed)
					.append(keepPhrase);

			return sb.toString();
		}

		@Override
		public int compareTo(Field o) {
			if ( this.sourceSeq == o.sourceSeq) return 0;
			if ( this.sourceSeq > o.sourceSeq) return 1;
			return -1;
		}
	}

	public Field field;
	
	public Map<Integer, Field> sourceSeqWithField = new HashMap<Integer, Field>();
	public Map<String, Field> nameWithField = new HashMap<String, Field>();
	public String tableName = null;
	public String familyName = null;
	public char fieldSeparator = '|';
	public String version = "0";
	
	@Deprecated
	public static FieldMapping getInstance(){
		return new FieldMapping();
	}
	
	static Map<String, FieldMapping> cache = new ConcurrentHashMap<String, FieldMapping>();
	public static FieldMapping getInstance(String fileLoc) throws ParseException{
		if ( cache.containsKey(fileLoc)) return cache.get(fileLoc);
		FieldMapping fm = new FieldMapping();
		fm.parseXML(fileLoc);
		if ( ! cache.containsKey(fileLoc)) cache.put(fileLoc, fm);
		return fm;
	}
	
	public void clear() {
		cache.clear();
	}
	
	public FieldMapping() {

	}

	public static void main(String[] args) throws IOException, SAXException,
			ParserConfigurationException, ParseException {
		
		FieldMapping fm = FieldMapping.getInstance(args[0]);
		fm.display();

	}
	
	int sourceSeq = 0;
	String name = null;
	
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
			tableName = (null == tableName) ? "hsearch" : (tableName.length() == 0 ? "hsearch" : tableName);
			
			familyName = attributes.getValue("familyName");
			familyName = (null == familyName) ? "1" : (familyName.length() == 0 ? "1" : familyName);

			String separator = attributes.getValue("fieldSeparator");
			if ( null == separator) fieldSeparator = '\t';
			else if ( separator.equals("\\t")) fieldSeparator = '\t';
			else if ( separator.equals("\\n")) fieldSeparator = '\n';
			else fieldSeparator = separator.charAt(0);

			String fldVal = attributes.getValue("version");
			version = (null == fldVal) ? "0" : (fldVal.length() == 0) ? "0" : fldVal;
			if ( null == fldVal ) IdSearchLog.l.info("Version is missing, Taking 0"); 
		}
		if (qName.equalsIgnoreCase("field")) {

			name = attributes.getValue("name");
			if ( null == name) IdSearchLog.l.warn("Missing Field Name(name) attribute");
			
			String sourceName = attributes.getValue("sourcename");

			String fldVal = attributes.getValue("sourcesequence");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Field Sequence(sourcesequence) attribute");
			sourceSeq = (null == fldVal || (fldVal.length() == 0)) ? -1 : Integer.parseInt(fldVal);
			
			fldVal = attributes.getValue("type");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Data Type(type) attribute");
			String dataType = fldVal;

			fldVal = attributes.getValue("mergekey");
			boolean isMergedKey = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");
			
			int mergePosition = (null == attributes.getValue("mergeposition") || (attributes.getValue("mergeposition").length() == 0)) 
								? -1 : Integer.parseInt(attributes.getValue("mergeposition"));
			
			fldVal = attributes.getValue("indexed");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Index (indexed) Setting to true");
			boolean isIndexable = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");

			fldVal = attributes.getValue("stored");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Store setting (stored) Setting to true");
			boolean isStored = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");
			
			fldVal = attributes.getValue("repeatable");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Repeat setting (repeatable) Setting to true");
			boolean isRepeatable = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");
			
			fldVal = attributes.getValue("skipNull");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Null setting (skipNull) Setting to true");
			boolean skipNull = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");
			
			fldVal = attributes.getValue("defaultValue");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Default Value (defaultValue) Setting to -");
			String defaultValue = ( null == fldVal) ? "-" : fldVal;
			
			fldVal = attributes.getValue("analyzer");
			String analyzer = ( null == fldVal) ? "" : fldVal;
			
			fldVal = attributes.getValue("analyzed");
			boolean isAnalyzed = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");
			boolean isDocIndex = (null == analyzer) ? false : analyzer.length() > 0;

			fldVal = attributes.getValue("isSeparable");
			if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " separablity (isSeparable) Setting to false");
			boolean isSeparable = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");
			
			fldVal = attributes.getValue("compress");
			boolean isCompressed = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");

			fldVal = attributes.getValue("cache");
			boolean isCachable = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");

			fldVal = attributes.getValue("keepphrase");
			boolean keepPhrase = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");

			field = new Field(name, sourceName, sourceSeq, isIndexable, isStored, isRepeatable,
					isMergedKey, mergePosition, skipNull, defaultValue, dataType, analyzer, 
					isDocIndex, isAnalyzed, isSeparable,isCachable, isCompressed, keepPhrase);
		}
	}

	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		if (qName.equalsIgnoreCase("field")) {
			sourceSeqWithField.put(sourceSeq, field);
			nameWithField.put(name, field);
		}
	}

	public void display() {
		for (Map.Entry<Integer, Field> entry : sourceSeqWithField.entrySet()) {
			System.out.println(entry.getKey() + " - "+ entry.getValue().toString());
		}
		if ( DEBUG_ENABLED )  HSearchLog.l.debug("sahema name is " + tableName);
	}
}
