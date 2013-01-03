package com.bizosys.hsearch.idsearch.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.idsearch.client.LoaderStep1_SourceDataCreator;
import com.oneline.ferrari.TestAll;

public class LoaderStep1_SourceDataCreatorTest extends TestCase 
{

	private LoaderStep1_SourceDataCreator sdc;
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
		
	public static void main(String[] args) throws Exception 
	{
		LoaderStep1_SourceDataCreatorTest t = new LoaderStep1_SourceDataCreatorTest();
		
		if ( modes[0].equals(mode) ) 
		{
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) 
		{
	        TestFerrari.testRandom(t);
		} else if  ( modes[2].equals(mode) ) 
		{
			t.setUp();
			t.testCreateSourceDataSQLException();
			t.testCreateSourceDataFileException();
			t.testCreateSourceData();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
		sdc = new LoaderStep1_SourceDataCreator();
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testCreateSourceDataSQLException()
	{
		try {
			sdc.createSourceData("SELECT FileReport_Id_n, File_Path_vc FROM i2iData.dbo.CMR_Study_FileRepor", 
					File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup");
			fail("Should fail with SQLException");
		} catch (Exception e) {
			assertEquals("Improper Exception is thrown", "Invalid object name 'i2iData.dbo.CMR_Study_FileRepor'.", e.getMessage());
		}
	}

	public void testCreateSourceDataFileException()
	{
		try {
			sdc.createSourceData("SELECT FileReport_Id_n, File_Path_vc FROM i2iData.dbo.CMR_Study_FileReport", 
					File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbku");
			fail("Should fail with File Exception");
		} catch (Exception e) {
			assertEquals("Improper Exception is thrown", "Parameter 'directory' is not a directory", e.getMessage());
		}
	}
	
	public void testCreateSourceData() throws Exception
	{
		File sourceDir = new File(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data");
		if(sourceDir.exists())
			sourceDir.delete();
		sdc.createSourceData("SELECT FileReport_Id_n, File_Path_vc FROM i2iData.dbo.CMR_Study_FileReport Where FileReport_Id_n = 8", 
				File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup");
		assertTrue("Source Data Directory is not created", sourceDir.exists());

		Iterator<File> iter = FileUtils.iterateFiles(new File(File.separator +"Work Documents" +File.separator +"i2i" 
						+File.separator +"reportsbkup" +File.separator +"source_data"), new String[]{"tsf"}, true);
		assertTrue("Source Data Files are not created", iter.hasNext());

		String fileAbsolutePath = "";
		while(iter.hasNext()) 
		{
		    File file = (File) iter.next();
		    String fileName = file.getParent().substring(file.getParent().lastIndexOf(File.separator)+1) +File.separator +file.getName();
		    assertEquals("Source data file names do not match", "source_data"+File.separator +"source_data_8.tsf", fileName);
		    fileAbsolutePath = file.getAbsolutePath();
		}
		BufferedReader br = new BufferedReader(new FileReader(fileAbsolutePath));
		String currentLine = br.readLine();
		assertNotNull("There is no data in the source file", currentLine);
	}
}
