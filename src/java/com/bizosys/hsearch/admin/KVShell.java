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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;

import com.bizosys.hsearch.hbase.HDML;
import com.bizosys.hsearch.kv.Searcher;
import com.bizosys.hsearch.kv.impl.FieldMapping;
import com.bizosys.hsearch.kv.impl.IEnricher;
import com.bizosys.hsearch.kv.impl.IndexerMapReduce;
import com.bizosys.hsearch.kv.impl.KVRowI;
import com.sun.tools.javac.Main;


public class KVShell {
	
	public static final String SRC_TEMP = "/tmp/src/";
	public static final String BUILD_TEMP = "/tmp/build/";
	public static final String JAR_TEMP = "/tmp/jar/";
	

	public List<String> queryFields = null;
	public List<KVRowI> resultStore = null; 
	public Searcher searcher = null;
	public PrintStream writer = null;
	public String customClasspath = null;
	
	public KVShell(PrintStream writer) throws IOException {
		queryFields = new ArrayList<String>();
		resultStore = new ArrayList<KVRowI>();
		this.writer = writer;
	}
	
	public KVShell() throws IOException {
		queryFields = new ArrayList<String>();
		resultStore = new ArrayList<KVRowI>();
		this.writer = System.out;
	}

	
	public static void main(String[] args) throws IOException {
		run(args, System.out);
		System.out.close();
	}

	@SuppressWarnings("static-access")
	public static void run(String[] args, PrintStream writer) throws IOException {

		String[] folders = new String[] {SRC_TEMP, BUILD_TEMP, JAR_TEMP};
		for (String folder : folders) {
			File tmpDir = new File(folder);
			if (!tmpDir.exists()) {
				tmpDir.mkdir();
			} else {
				if (!tmpDir.isDirectory()) {
					writer.println("Found [" + folder + "] file , Expecting [" + folder + "] directory.");
					return;
				}
			}
			
		}

		Option create = OptionBuilder.withArgName("schemaFile").hasArgs(1)
									.withDescription("Specify Schema file.")
									.create("create");

		Option load = OptionBuilder.withArgName("paths").hasArgs(2)
									.withDescription("Specify data path and schema file.")
									.create("load");
		
		Option search = OptionBuilder.withArgName("queries").hasArgs(4)
									.withDescription("Specify schema file path and queries.")
									.create("search");

		Option sort = OptionBuilder.withArgName("sort queries").hasArgs(1)
									.withDescription("Specify the sorting order.")
									.create("sort");

		Options options = new Options();
		options.addOption(create);
		options.addOption(load);
		options.addOption(search);
		options.addOption(sort);

		KVShell shell = new KVShell(writer);
		CommandLineParser parser = new GnuParser();
		try {
			CommandLine commandLine = parser.parse(options, args);
			if (commandLine.hasOption("create")) {
				String[] arguments = commandLine.getOptionValues("create");
				shell.createTable(arguments);
				return;
			}

			if (commandLine.hasOption("load")) {
				String[] arguments = commandLine.getOptionValues("load");
				shell.loadTable(arguments);
				return;
			}
			if (commandLine.hasOption("search")) {
				String[] arguments = commandLine.getOptionValues("search");
				shell.search(arguments);
				return;
			}
			if (commandLine.hasOption("sort")) {
				String[] arguments = commandLine.getOptionValues("sort");
				shell.sort(arguments[0]);
				return;
			}
			
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "hsearch", options, true);

		} catch (ParseException exp) {
			throw new IOException("Parsing failed.  Reason: ", exp);
		}
	}

	public void createTable(String[] arguments) throws IOException {
		
		FieldMapping fm = null;
		try {
			//read schema file from hadoop
			StringBuilder sb = new StringBuilder();
			try {
				Path hadoopPath = new Path(arguments[0]);
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while ((line = br.readLine()) != null) {
					sb.append(line);
				}
			} catch (Exception e) {
				e.printStackTrace(System.err);
				throw new IOException("Cannot read from path " + arguments[0], e);
			}

			String schemaStr = sb.toString();
			fm = FieldMapping.getXMLStringFieldMappings(schemaStr);
			
			List<HColumnDescriptor> colFamilies = new ArrayList<HColumnDescriptor>();
			HColumnDescriptor cols = new HColumnDescriptor(fm.familyName.getBytes());
			colFamilies.add(cols);
			HDML.create(fm.tableName, colFamilies);

		}catch (Exception e) {
			e.printStackTrace(System.err);
			throw new IOException("Error creating table [" + fm.tableName + "]", e);
		}
	}
		
	public void loadTable(String[] arguments) throws IOException {
		try {
			//read schema file from hadoop
			StringBuilder sb = new StringBuilder();
			try {
				Path hadoopPath = new Path(arguments[1]);
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while ((line = br.readLine()) != null) {
					sb.append(line);
				}
			} catch (Exception e) {
				writer.println("Cannot read from path " + arguments[1]);
				throw new IOException(e);
			}

			String schemaStr = sb.toString();
			FieldMapping fm = FieldMapping.getXMLStringFieldMappings(schemaStr);
			
			String[] indexerDetail = new String[]{arguments[0],arguments[1],fm.tableName};
			IndexerMapReduce.main(indexerDetail);

			ColGenerator.generate(fm, SRC_TEMP);

			HSearchShell hShell = new HSearchShell();
			File javaSourceDirectory = new File(SRC_TEMP);
			List<String> sources = new ArrayList<String>();
			hShell.listAllFiles(javaSourceDirectory, sources, ".java");
						
			String[] compileArgs = new String[sources.size() + 4];
			int index = 0;
			compileArgs[index++] = "-cp";
			compileArgs[index++] = ( null != customClasspath) ? 
					customClasspath + ":" +  System.getProperty("java.class.path") :
					System.getProperty("java.class.path");
			 
			compileArgs[index++] = "-d";
			compileArgs[index++] = BUILD_TEMP;
			for (String source : sources) {
				compileArgs[index++] = source;
			}
			
			//compile
			Main.compile(compileArgs);

			// make jar
			Manifest manifest = new Manifest();
			manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION,"1.0");
			manifest.getMainAttributes().put(Attributes.Name.MAIN_CLASS,"Column");
			JarOutputStream target = new JarOutputStream(new FileOutputStream(JAR_TEMP + "/column.jar"), manifest);

			writer.println("Building Jar : ");
			hShell.createJar(new File(BUILD_TEMP), target);
			target.close();
			
		} catch (Exception e) {
			e.printStackTrace(System.err);
			throw new IOException("Loading to table Failure", e);
		}
	}

	public void search(String[] arguments) throws IOException {
		URL jarUrl = null;
		try {
			
			jarUrl = new File("/tmp/jar/column.jar").toURI().toURL();
			URLClassLoader cl = URLClassLoader.newInstance(new URL[] { jarUrl },  this.getClass().getClassLoader());

			Class<?> Column = cl.loadClass("Column");
			Object column = Column.newInstance();
			KVRowI blankRow = (KVRowI)column;
			searchOnObject(arguments, blankRow);
		} catch (Exception ex) {
			ex.printStackTrace(System.err);
			throw new IOException("Url " + jarUrl.toString(), ex);
		}
	}
	
	public void searchOnObject(String[] arguments, KVRowI blankRow) throws IOException {
		try {
			
			StringBuilder sb = new StringBuilder();
			Path hadoopPath = new Path(arguments[0]);
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
			String line = null;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
			
			String schemaStr = sb.toString();
			FieldMapping fm = FieldMapping.getXMLStringFieldMappings(schemaStr);

			IEnricher enricher = null;
			if(null == searcher)searcher = new Searcher("kv-store", fm);
			
			searcher.search(fm.tableName, arguments[1], arguments[2], arguments[3], blankRow, enricher);
			String[] selectFields = arguments[2].split(",");
			resultStore = searcher.getResult();
			for (KVRowI aRow : resultStore) {
				for (String selectField : selectFields) {
					writer.print(aRow.getValue(selectField) + "\t");
				}
				writer.println();
			}
		} catch (Exception e) {
			e.printStackTrace(System.err);
			throw new IOException("Search Failure", e);
		}
	}

	public void sort(String sorters) throws IOException {
		try {
			String[] sorterA = sorters.split(",");
			List<KVRowI> data = searcher.sort(sorterA);
			int index = 0;
			for (KVRowI aRow : data) {
				writer.println(aRow.getValue(queryFields.get(index++)) + "\t" + aRow.getValue(queryFields.get(index++)) + "\t" + aRow.getValue(queryFields.get(index++))+ "\t" + aRow.getValue(queryFields.get(index++)));
				index = 0;
			}

		} catch (Exception e) {
			e.printStackTrace(System.err);
			throw new IOException("Sort Failure", e);
		}
	}

	public void showFacet(String facetQuery){
		String[] facetA = facetQuery.split(",");
		List<KVRowI> data = searcher.getResult();
		for (KVRowI aRow : data) {
			for (String facet : facetA) {
				writer.print(aRow.getValue(facet) + "\t");				
			}
			writer.println();
		}
	}

	public void clear(){
		if(null != queryFields)queryFields.clear();
		if(null != searcher)searcher = null;
	}

	public void parseQuery(String query, List<String> queryFields) {
		
		String skeletonQuery = query.replaceAll("\\s+", " ").replaceAll("\\(", "").replaceAll("\\)", "");
		String[] splittedQueries = skeletonQuery.split("( AND | OR | NOT )");
		int colonIndex = -1;
		String fieldName = "";
		
		for (String splittedQuery : splittedQueries) {
			splittedQuery = splittedQuery.trim();
			colonIndex = splittedQuery.indexOf(':');
			fieldName = splittedQuery.substring(0,colonIndex);
			queryFields.add(fieldName);
		}
	}

}
