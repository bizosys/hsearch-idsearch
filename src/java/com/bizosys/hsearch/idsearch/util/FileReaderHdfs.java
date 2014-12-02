/**
 *    Copyright 2014, Bizosys Technologies Pvt Ltd
 *
 *    This software and all information contained herein is the property
 *    of Bizosys Technologies.  Much of this information including ideas,
 *    concepts, formulas, processes, data, know-how, techniques, and
 *    the like, found herein is considered proprietary to Bizosys
 *    Technologies, and may be covered by U.S., India and foreign patents or
 *    patents pending, or protected under trade secret laws.
 *    Any dissemination, disclosure, use, or reproduction of this
 *    material for any reason inconsistent with the express purpose for
 *    which it has been disclosed is strictly forbidden.
 *
 *                        Restricted Rights Legend
 *                        ------------------------
 *
 *    Use, duplication, or disclosure by the Government is subject to
 *    restrictions as set forth in paragraph (b)(3)(B) of the Rights in
 *    Technical Data and Computer Software clause in DAR 7-104.9(a).
 */

package com.bizosys.hsearch.idsearch.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.Utils;

public class FileReaderHdfs {

	public static String convertToString(final String path, Configuration conf, final char separator) throws IOException{

		StringBuilder appender = new StringBuilder(65536);

		if(null == conf)
			conf = new Configuration();

		BufferedReader reader = null;
		try {
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(conf);

			if ( fs.exists(hadoopPath) ) {
				reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				boolean isFirst = true;
				while((line = reader.readLine())!=null) {

					if(isFirst)
						isFirst = false;
					else
						appender.append(separator);

					appender.append(line);
				}
			} else {
				throw new IOException("File Does not exists on HDFS.");
			}

		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(fex);
		} catch (Exception pex) {
			System.err.println("Error : " + path);
			throw new IOException(pex);
		} finally{
			try {if ( null != reader ) reader.close();} catch (Exception ex) {}
		}

		String value = appender.toString();
		appender.delete(0, 65535);
		return value;
	}

	/**
	 * Loads a HDFS file lines
	 * @param path	HDFS file path
	 * @param conf	Hadoop configuration
	 * @return	Lines
	 * @throws IOException
	 */
	public static List<String> toLines(final String path, Configuration conf) throws IOException{

		BufferedReader reader = null;
		List<String> lines = new ArrayList<String>();

		try {
			if(null == conf) conf = new Configuration();
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			if ( fs.exists(hadoopPath) ) {
				reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while((line = reader.readLine())!=null) {
					if (line.length() == 0) continue;
					lines.add(line);	
				}
			} else {
				throw new IOException("File Does not exists on HDFS OR s3.");
			}

		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(fex);
		} catch (Exception pex) {
			System.err.println("Error : " + path);
			throw new IOException(pex);
		} finally{
			try {if ( null != reader ) reader.close();} catch (Exception ex) {}
		}
		return lines;
	}

	/**
	 * Loads a HDFS file lines
	 * @param path	HDFS file path
	 * @param conf	Hadoop configuration
	 * @return	Lines
	 * @throws IOException
	 */
	public static String toString(final String path, Configuration conf, boolean readLocalFileSystem) throws IOException{

		BufferedReader reader = null;
		StringBuilder appender = new StringBuilder(1024);
		try {
			if(null == conf) conf = new Configuration();
			Path hadoopPath = new Path(path);
			FileSystem fs = FileSystem.get(conf);
			if ( fs.exists(hadoopPath) ) {
				reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
				String line = null;
				while((line = reader.readLine())!=null) {
					if (line.length() == 0) continue;
					appender.append(line);
				}
			} else {
				if( readLocalFileSystem )
					return toString(path);
				else
					throw new IOException("File Does not exists on HDFS : " + path);
			}

		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + path);
			throw new IOException(fex);
		} catch (Exception pex) {
			System.err.println("Error : " + path);
			throw new IOException(pex);
		} finally{
			try {if ( null != reader ) reader.close();} catch (Exception ex) {}
		}
		return appender.toString();
	}

	public static String toString(String fileName) throws Exception 
	{

		File aFile = new File(fileName);
		BufferedReader reader = null;
		InputStream stream = null;
		StringBuilder sb = new StringBuilder();
		try {
			stream = new FileInputStream(aFile); 
			reader = new BufferedReader ( new InputStreamReader (stream) );
			String line = null;
			String newline = getLineSeaprator();
			while((line=reader.readLine())!=null) {
				if (line.length() == 0) continue;
				sb.append(line).append(newline);	
			}
			return sb.toString();
		} 
		catch (Exception ex) 
		{
			throw new RuntimeException(ex);
		} 
		finally 
		{
			try {if ( null != reader ) reader.close();
			} catch (Exception ex) {throw ex;}
			try {if ( null != stream) stream.close();
			} catch (Exception ex) {throw ex;}
		}
	}

	public static String getLineSeaprator() {
		String seperator = System.getProperty ("line.seperator");
		if ( null == seperator) return "\n";
		else return seperator;
	}

	/**
	 * Loads a HDFS file lines
	 * @param path	HDFS file path
	 * @param conf	Hadoop configuration
	 * @return	Lines
	 * @throws IOException
	 */
	public static List<String> toLinesMROutput(final String outputFolder, Configuration conf) throws IOException{

		if(null == conf)
			conf = new Configuration();

		BufferedReader reader = null;
		List<String> lines = new ArrayList<String>(128);

		try {

			Path outputPath = new Path(outputFolder);
			FileSystem fs = FileSystem.get(conf);
			PathFilter pathFilter = new Utils.OutputFileUtils.OutputFilesFilter();
			FileStatus[] fileStatuses = fs.listStatus(outputPath, pathFilter); 
			for (FileStatus fileStatus : fileStatuses) {
				Path hadoopPath = fileStatus.getPath();
				if ( fs.exists(hadoopPath) ) {
					reader = new BufferedReader(new InputStreamReader(fs.open(hadoopPath)));
					String line = null;
					while((line = reader.readLine())!=null) {
						if (line.length() == 0) continue;
						lines.add(line);	
					}
				} else {
					throw new IOException("File Does not exists on HDFS.");
				}				
			}
		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + outputFolder);
			throw new IOException(fex);
		} catch (Exception pex) {
			System.err.println("Error : " + outputFolder);
			throw new IOException(pex);
		} finally{
			try {if ( null != reader ) reader.close();} catch (Exception ex) {}
		}
		return lines;
	}

	/**
	 * Loads a HDFS file lines
	 * @param path	HDFS file path
	 * @param conf	Hadoop configuration
	 * @return	Lines
	 * @throws IOException
	 */
	public static Map<String, String> toNameValues(final String outputFolder, Configuration conf , final char sepearator) throws IOException{

		if(null == conf) conf = new Configuration();

		BufferedReader reader = null;
		Map<String, String> mapNameValue = new HashMap<String, String>(3);

		try {

			Path outputPath = new Path(outputFolder);
			FileSystem fs = FileSystem.get(conf);
			if ( fs.exists(outputPath) ) {

				reader = new BufferedReader(new InputStreamReader(fs.open(outputPath)));
				String line = null;
				int index = 0;

				while((line = reader.readLine())!=null) {

					if (line.length() == 0) continue;

					char first=line.charAt(0);
					switch (first) {
					case ' ' : case '\n' : case '#' :  // skip blank & comment lines
						continue;
					}
					index = line.indexOf(sepearator);
					mapNameValue.put(line.substring(0, index) , line.substring(index+1));
				}
			}
		} catch (FileNotFoundException fex) {
			System.err.println("Cannot read from path " + outputFolder);
			throw new IOException(fex);
		} catch (Exception pex) {
			System.err.println("Error : " + outputFolder);
			throw new IOException(pex);
		} finally{
			try {if ( null != reader ) reader.close();} catch (Exception ex) {}
		}

		return mapNameValue;
	}
}