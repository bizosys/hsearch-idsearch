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
package com.bizosys.hsearch.admin;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.treetable.compiler.FileWriterUtil;

public class ColGenerator {

	static Map<String, Character> dataTypesPrimitives = new HashMap<String, Character>();
	
	static {
		dataTypesPrimitives.put("string", 't');
		dataTypesPrimitives.put("int", 'i');
		dataTypesPrimitives.put("float", 'f');
		dataTypesPrimitives.put("double", 'd');
		dataTypesPrimitives.put("long", 'l');
		dataTypesPrimitives.put("short", 's');
		dataTypesPrimitives.put("boolean", 'b');
		dataTypesPrimitives.put("byte", 'c');
	}

	public static void generate(final FieldMapping fm , final String path, final String completeClassName) throws IOException {

		String template = getTextFileContent("Column.tmp");
				
		String params = "";
		String setters = "";
		String getters = "";
		String gettersNative = "";
		String stringSequencer = "";
		String intSequencer = "";
		String floatSequencer = "";
		String doubleSequencer = "";
		String longSequencer = "";
		String byteSequencer = "";
		String booleanSequencer = "";
		String shortSequencer = "";
		
		for (Map.Entry<String, Field> entry : fm.nameWithField.entrySet()) {
			FieldMapping.Field fld = entry.getValue();
			if(!fld.isStored) continue;
			
			String casted ="";
			String fieldValue = "";
			
			String dataType = fld.getDataType().toLowerCase();
			char dataTypeChar = dataTypesPrimitives.get(dataType);

			switch (dataTypeChar) {
				case 't':
				{
					fieldValue = " = null";
					casted = "value.toString()";		
					stringSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";				
				}
				break;
				case 'i':
				{
					fieldValue = " = 0";
					casted = "(Integer)value";
					intSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";								
				}
				break;
				case 'f':
				{
					fieldValue = " = 0.0f";
					casted = "(Float)value";
					floatSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";				
				}
				break;
				case 'd':
				{
					fieldValue = " = 0.0";
					casted = "(Double)value";
					doubleSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";								
				}
				break;
				case 'l':
				{
					fieldValue = " = 0L";
					casted = "(Long)value";
					longSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";				
				}
				break;
				case 's':
				{
					fieldValue = " = 0";
					casted = "(Short)value";
					shortSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";				
				}
				break;
				case 'b':
				{
					fieldValue = " = false";
					casted = "(Boolean)value";
					booleanSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";				
				}
				break;
				case 'c':
				{
					fieldValue = " = 0";
					casted = "(Byte)value";
					byteSequencer += "\tcase "+ fld.sourceSeq + ":\n\t\t return this."+fld.name.toLowerCase() +";\n";
				}
				break;
			}

			params += "\tpublic " + fld.getDataType() + " " + fld.name.toLowerCase() + fieldValue +";\n";
			setters += "\t\tcase "+ fld.sourceSeq + ":\n\t\t\t this."+fld.name.toLowerCase()+" = " + casted + ";\n\t\t break;\n";
			getters += "\t\tcase "+ fld.sourceSeq + ":\n\t\t\t return this."+fld.name.toLowerCase()+";\n";
			gettersNative += "\t\tcase "+ fld.sourceSeq + ":\n\t\t\t return new TypedObject(this."+fld.name.toLowerCase()+");\n";
			
		}
		
		int index = completeClassName.lastIndexOf('.');
		String pkg = "";
		String className = completeClassName;
		if(index > 0){
			pkg = completeClassName.substring(0,index);
			className = completeClassName.substring(index + 1);
		}
		
		if(0 == pkg.length())
			template = template.replace("--PACKAGE_NAME--", "");
		else
			template = template.replace("--PACKAGE_NAME--", "package " + pkg + ";");
		template = template.replace("--COLUMN-NAME--", className);
		template = template.replace("--PARAMS--", params);
		template = template.replace("--SETTERS--", setters);
		template = template.replace("--GETTERS--", getters);
		template = template.replace("--GETTERS_NATIVE--", gettersNative);
		template = template.replace("--INTEGER_SEQUENCER--", intSequencer);
		template = template.replace("--FLOAT_SEQUENCER--", floatSequencer);
		template = template.replace("--STRING_SEQUENCER--", stringSequencer);
		template = template.replace("--DOUBLE_SEQUENCER--", doubleSequencer);
		template = template.replace("--LONG_SEQUENCER--", longSequencer);
		template = template.replace("--BOOLEAN_SEQUENCER--", booleanSequencer);
		template = template.replace("--SHORT_SEQUENCER--", shortSequencer);
		template = template.replace("--BYTE_SEQUENCER--", byteSequencer);
		
		//System.out.println(template);
		FileWriterUtil.downloadToFile(template.getBytes(),new File(path + className + ".java") );
	}

	public static String getTextFileContent(String fileName) throws IOException {
		InputStream stream = null; 
		Reader reader = null; 
		
		try {
			stream = ColGenerator.class.getResourceAsStream(fileName);
			
			reader = new BufferedReader ( new InputStreamReader (stream) );

	        byte[] bytes = new byte[1024]; // Create the byte array to hold the data
	        int numRead = 0;
	        
	        StringBuilder sb = new StringBuilder();
	        while (true) {
	        	numRead = stream.read(bytes, 0, 1024);
	        	if ( numRead == -1 ) break;
	        	
	        	sb.append(new String(bytes, 0, numRead));
	        }
	        
	        return sb.toString();
	        
		} finally {
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {ex.printStackTrace(System.err);}
		}
	}
	
	public static void createVO(final FieldMapping fm, final String outputPath, final String completeClassName){
		try {
			ColGenerator.generate(fm, outputPath, completeClassName);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public static void main(String[] args) throws IOException, ParseException {
		FieldMapping fm = FieldMapping.getInstance();
		if ( args.length < 1) {
			File file = new File("./src/java/com/bizosys/hsearch/admin/schema.xml");
			if ( ! file.exists()) {
				System.err.println("java ColGenerator ./schema.xml");
				System.exit(1);
			}
		}
		ColGenerator.generate(fm, "/tmp", "Column");
	}

}
