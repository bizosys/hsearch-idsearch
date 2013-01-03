package com.bizosys.hsearch.idsearch.client;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import com.oneline.dao.PoolFactory;
import com.oneline.dao.ReadTwin;
import com.oneline.dao.ReadTwin.Twin;
import com.oneline.util.FileReaderUtil;

public class LoaderStep1_SourceDataCreator 
{
	private static Pattern pattern = Pattern.compile("[^\\p{ASCII}]");
	private Map<String, String> fileNameMap = new HashMap<String, String>();
	private final String documentDirName = File.separator +"source_data";
	private final String documentPrefix = File.separator +"source_data_";
	private final String documentSuffix = ".tsf";

	public void createSourceData(String documentQuery, String reportFilePath) throws SQLException, FileNotFoundException, IOException, SAXException, TikaException, URISyntaxException 
	{
        PoolFactory.getInstance().setup(FileReaderUtil.toString("db.conf"));
		Iterator<File> iter = FileUtils.iterateFiles(new File(reportFilePath), new String[]{"doc"}, true);
		while(iter.hasNext()) 
		{
		    File file = (File) iter.next();
			System.out.println("Inside the Iterator Loop: " +file.getAbsolutePath());
		    String fileName = file.getParent().substring(file.getParent().lastIndexOf(File.separator)+1) +File.separator +file.getName();
			System.out.println("Inside the Iterator Loop: " +fileName);
		    String fileAbsolutePath = file.getAbsolutePath();
		    
		    fileNameMap.put(fileName, fileAbsolutePath);
		}
		
		List<Twin> fileResults = new ReadTwin().execute(documentQuery);
		String fileData = "";
		for (Twin twin : fileResults) 
		{
			if(fileNameMap.get(twin.second) != null)
			{
				File reportFile = new File(fileNameMap.get(twin.second));
				if(reportFile.exists())
				{
					InputStream input = new FileInputStream(reportFile);
					ContentHandler textHandler = new BodyContentHandler();
					Metadata metadata = new Metadata();
					ParseContext context = new ParseContext();
					Parser parser = new AutoDetectParser();
					parser.parse(input, textHandler, metadata, context);
					input.close();
					
					String rawText = textHandler.toString();
					String fileText = stripNonAscii(rawText);
					fileData = twin.first +"\t" +fileText;
					createTabSeparatedFile(reportFilePath, Integer.parseInt(twin.first.toString()), fileData);
				}
				else
				{
					System.out.println("File not found at the prescribed path. File path: " +twin.second);
				}
			}
		}
	}

	private String stripNonAscii(String rawText) 
	{
		String fileText = rawText.replace("\t", "-");
		fileText = fileText.replace("\u2029", " ");
		fileText = fileText.replace("\n", "[-]");
		fileText = fileText.replace("\r", "[-]");
		fileText = fileText.replace("\n\r", "[-]");
		Matcher titleMatcher = pattern.matcher(fileText);
		fileText = titleMatcher.replaceAll(fileText);
		return fileText;
	}
	
	private void createTabSeparatedFile(String reportFilePath, int documentId, String dataToWrite)
	{
		try
		{
			File sourceDir = new File(reportFilePath +documentDirName);
			if (sourceDir.exists() || sourceDir.mkdirs()) 
			{
				File tsvFile = new File(sourceDir +documentPrefix +documentId +documentSuffix);
				if(!tsvFile.exists())
				{
					tsvFile.createNewFile();
				}
				FileWriter fstream = new FileWriter(tsvFile, true);
				BufferedWriter out = new BufferedWriter(fstream, (64*1024));
				out.write(dataToWrite);
				out.close();
				fstream.close();
			}
			else
			{
				System.out.println("Could not creating directory. Abandoning source loader.");
			}
			
		}
		catch (Exception e)
		{
			System.err.println("Error: " + e.getMessage());
		}	
	}
	
	public static void main(String[] args) throws Exception 
	{
		LoaderStep1_SourceDataCreator sdc = new LoaderStep1_SourceDataCreator();
		sdc.createSourceData("SELECT FileReport_Id_n, File_Path_vc FROM i2iData.dbo.CMR_Study_FileReport", 
				File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup");
	}
}
