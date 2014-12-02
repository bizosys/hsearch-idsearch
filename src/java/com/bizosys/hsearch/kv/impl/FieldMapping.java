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

import com.bizosys.hsearch.idsearch.util.FileReaderHdfs;
import com.bizosys.hsearch.idsearch.util.IdSearchLog;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.unstructured.AnalyzerFactory;

/**
 * The hsearch index schema file is parsed by this class.
 * @author shubhendu
 *
 */
public class FieldMapping extends DefaultHandler {

	private static final boolean INFO_ENABLED = IdSearchLog.l.isInfoEnabled();

	private static final boolean DEBUG_ENABLED = IdSearchLog.l.isDebugEnabled();

	/**
	 * Representation of Single field in schema.
	 * @author shubhendu
	 *
	 */
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
		public String dataType;
		private byte dataTypeCode;
		public String analyzer;
		public boolean isAnalyzed;
		public boolean isCompressed;
		
		public  boolean isPorous;
		public  boolean isFilter;
		public  boolean isAggregate;

		public boolean isBiWord;
		public boolean isTriWord;
		
		public boolean isDocIndex;
		public boolean isCachable = true;
		public boolean isSourceUrl = false;
		public String expression = null;
		
		public Field() {
		}
		
		public String getDataType() {
			return dataType;
		}
		
		public void setDataTypeCode(byte code){
			this.dataTypeCode = code; 
		}
		
		public byte getDataTypeCode(){
			return this.dataTypeCode; 
		}


		/**
		 * Create a new field instance.
		 * 
		 * @param name
		 * @param sourceName
		 * @param sourceSeq
		 * @param isIndexable
		 * @param isStored
		 * @param isRepeatable
		 * @param isMergedKey
		 * @param mergePosition
		 * @param skipNull
		 * @param defaultValue
		 * @param fieldType
		 * @param analyzer
		 * @param isDocIndex
		 * @param isAnalyzed
		 * @param isCachable
		 * @param isCompressed
		 * @param biWord
		 * @param triWord
		 * @param isSourceUrl
		 * @param expression
		 */
		public Field(String name,String sourceName, int sourceSeq, boolean isIndexable,boolean isStored,
				boolean isRepeatable, boolean isMergedKey, int mergePosition,
				boolean skipNull, String defaultValue, String fieldType, String analyzer, 
				boolean isDocIndex, boolean isAnalyzed, boolean isCachable,
				boolean isCompressed, boolean biWord, boolean triWord, 
				boolean isSourceUrl, String expression) {
			
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
			
			this.isCachable = isCachable;
			this.isCompressed = isCompressed;
			this.isBiWord = biWord;
			this.isTriWord = triWord; 
			
			this.expression = expression;
			this.isSourceUrl = isSourceUrl;
		}

		public String toString() {

			StringBuilder sb = new StringBuilder();
			sb.append(sourceName).append('\t').append(name).append('\t').append(sourceSeq).append('\t')
					.append(isIndexable).append('\t').append(isRepeatable)
					.append('\t').append(isMergedKey).append('\t')
					.append(skipNull).append('\t').append(defaultValue)
					.append('\t').append(dataType).append('\t').append(analyzer)
					.append('\t').append(isCachable).append('\t').append(isCompressed)
					.append(isBiWord).append('\t').append(isTriWord).append('\t').append(expression);

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
	
	/**
	 * This sourceSeqWithField container contains the mapping of 
	 * schema source sequencenumber and the corresponding Field
	 */
	public Map<Integer, Field> sourceSeqWithField = new HashMap<Integer, Field>();

	/**
	 * This sourceSeqWithField container contains the mapping of 
	 * schema name attribute and the corresponding Field
	 */
	public Map<String, Field> nameWithField = new HashMap<String, Field>();
	
	public String tableName = null;
	public String familyName = null;
	public String voClass = null;
	public char fieldSeparator = '|';
	public String version = "0";
	public boolean append = false;
	public boolean delete = false;
	public  int skewPoint = -1;
	public  int internalKey = -1;
	
	
	/**
	 * Returns FieldMapping instance.
	 * This is @deprecated method Please use getInstance(String fileLoc)  
	 * @return FieldMapping instance
	 */
	@Deprecated
	public static FieldMapping getInstance(){
		return new FieldMapping();
	}

	/**
	 * This container caches the different field mapping instance 
	 * for different schemas for subsequent reuse.
	 */
	static Map<String, FieldMapping> cache = new ConcurrentHashMap<String, FieldMapping>();
	
	/**
	 * Returns a FieldMapping instance for a given schema file.
	 * @param fileLoc
	 * @return FieldMapping instance
	 * @throws ParseException
	 */
	public static FieldMapping getInstance(String fileLoc) throws ParseException{
		if ( cache.containsKey(fileLoc)) return cache.get(fileLoc);
		FieldMapping fm = new FieldMapping();
		fm.parseXML(fileLoc);
		if ( ! cache.containsKey(fileLoc)) cache.put(fileLoc, fm);
		return fm;
	}
	/**
	 * Clears the cache of FieldMapping instances.
	 */
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
	
	/**
	 * Given string content of schema file it parses xml content.
	 * @param xmlString
	 * @throws ParseException
	 */
	public void parseXMLString(final String xmlString) throws ParseException{
		SAXParserFactory saxFactory = SAXParserFactory.newInstance();
		SAXParser saxParser;
		try {

			saxParser = saxFactory.newSAXParser();
			saxParser.parse(new InputSource(new StringReader(xmlString)), this);
			
			AnalyzerFactory.getInstance().init(this);
			
		} catch (Exception e) {
			IdSearchLog.l.fatal("File Path: ", e);
			throw new ParseException(e.getMessage(), 0);
		}
	}
	
	/**
	 * Parses the given schema file.
	 * @param filePath
	 * @throws ParseException
	 */
	public void parseXML(final String filePath) throws ParseException {
		try {
			
			String schemaData = FileReaderHdfs.toString(filePath, null, true);
			parseXMLString(schemaData);
			
		} catch (Exception e) {
			IdSearchLog.l.fatal("Error parsing schema from file : " + filePath, e );
			throw new ParseException(e.getMessage(), 0);
		}
	}
	
	/**
	 * For each field in the schema this method is executed.
	 * Sets the default value for those attributes that are not present in
	 * the schema file.
	 * The Default values are :
	 * isMergedKey = false
	 * mergePosition = -1
	 * isIndexable = true
	 * isStored = true
	 * isRepeatable = true
	 * skipNull = true
	 * fldVal = - (string) 0 (others) 
	 * analyzer = EMPTY
	 * isDocIndex = false
	 * isAnalyzed = false
	 * isCompressed = false
	 * isCachable = true
	 * biWord = false
	 * triWord = false
	 * sourceurl = false
	 */
	public void startElement(String uri, String localName, String qName,Attributes attributes) throws SAXException {

		if (qName.equalsIgnoreCase("field")) {

			String sourceName = null;
			String dataType = null;
			String defaultValue = null;
			String analyzer = null;
			String expr = null;
			
			try {
				name = attributes.getValue("name");
				if ( null == name) IdSearchLog.l.warn("Missing Field Name(name) attribute");
				
				sourceName = attributes.getValue("sourcename");

				String fldVal = attributes.getValue("sourcesequence");
				if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Field Sequence(sourcesequence) attribute");
				sourceSeq = (null == fldVal || (fldVal.length() == 0)) ? -1 : Integer.parseInt(fldVal);
				
				fldVal = attributes.getValue("type");
				if ( null == fldVal) IdSearchLog.l.debug("Missing " + name + " Data Type(type) attribute");
				dataType = fldVal;

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
				if ( ! skipNull ) {
					if ( dataType.toLowerCase().equals("string") ) {
						if ( null == fldVal) IdSearchLog.l.debug(
							"Missing " + name + " Default Value (defaultValue) Setting to -");
						fldVal = ( null == fldVal) ? "-" : fldVal;
					} else {
						boolean isDefaultNull = (fldVal == null) ? true : (fldVal.trim().length() == 0 );
						if ( isDefaultNull ) fldVal = "0";
					}
				}
				defaultValue = fldVal;
				
				fldVal = attributes.getValue("analyzer");
				analyzer = ( null == fldVal) ? "" : fldVal;
				
				fldVal = attributes.getValue("analyzed");
				boolean isAnalyzed = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");
				boolean isDocIndex = (null == analyzer) ? false : analyzer.length() > 0;

				fldVal = attributes.getValue("compress");
				boolean isCompressed = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");

				fldVal = attributes.getValue("cache");
				boolean isCachable = ( null == fldVal) ? true : fldVal.equalsIgnoreCase("true");

				fldVal = attributes.getValue("biword");
				boolean biWord = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");

				fldVal = attributes.getValue("triword");
				boolean triWord = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");

				fldVal = attributes.getValue("expression");
				expr = ( null == fldVal) ? null : (fldVal.length() == 0 ) ? null : fldVal;

				fldVal = attributes.getValue("sourceurl");
				boolean sourceurl = ( null == fldVal) ? false : fldVal.equalsIgnoreCase("true");
				
				field = new Field(name, sourceName, sourceSeq, isIndexable, isStored, isRepeatable,
						isMergedKey, mergePosition, skipNull, defaultValue, dataType, analyzer, 
						isDocIndex, isAnalyzed, isCachable, isCompressed, biWord, triWord, sourceurl, expr);
				
				byte dataTypecode = KVDataSchema.getDataTypeMapping(field);
				field.setDataTypeCode(dataTypecode);

			} catch (Exception e) {
				System.err.println(
						"Name=" + name + "\tSourcename=" + sourceName + 
						"\tDatatype=" + dataType + "\tDefaultValue=" + defaultValue + 
						"\tAnalyzer=" + analyzer + "\tExpression=" + expr);
				
				throw new SAXException(e);
			}
		
		} else if(qName.equalsIgnoreCase("schema")){
			tableName = attributes.getValue("tableName");
			tableName = (null == tableName) ? "hsearch" : (tableName.length() == 0 ? "hsearch" : tableName);
			
			familyName = attributes.getValue("familyName");
			familyName = (null == familyName) ? "1" : (familyName.length() == 0 ? "1" : familyName);

			voClass = attributes.getValue("voClass");

			String separator = attributes.getValue("fieldSeparator");
			if ( null == separator) fieldSeparator = '\t';
			else if ( separator.equals("\\t")) fieldSeparator = '\t';
			else if ( separator.equals("\\n")) fieldSeparator = '\n';
			else fieldSeparator = separator.charAt(0);

			String fldVal = attributes.getValue("version");
			version = (null == fldVal) ? "0" : (fldVal.length() == 0) ? "0" : fldVal;
			if ( null == fldVal ) IdSearchLog.l.info("Version is missing, Taking 0"); 
			
			fldVal = attributes.getValue("append");
			append = (null == fldVal) ? false : fldVal.equals("true");
			
			fldVal = attributes.getValue("delete");
			delete = (null == fldVal) ? false : fldVal.equals("true");
			
			fldVal = attributes.getValue("skewPoint");
			skewPoint = (null == fldVal || (fldVal.length() == 0)) ? -1 : Integer.parseInt(fldVal);
			
			fldVal = attributes.getValue("internalKey");
			internalKey = (null == fldVal || (fldVal.length() == 0)) ? -1 : Integer.parseInt(fldVal); 

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
			if ( INFO_ENABLED ) IdSearchLog.l.info(entry.getKey() + " - "+ entry.getValue().toString());
		}
		if ( DEBUG_ENABLED )  IdSearchLog.l.debug("sahema name is " + tableName);
	}
}
