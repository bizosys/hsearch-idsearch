package com.bizosys.hsearch.idsearch.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.idsearch.client.LoaderStep2_TokenFileCreator;
import com.oneline.ferrari.TestAll;

public class LoaderStep2_TokenFileCreatorTest extends TestCase 
{

	private LoaderStep2_TokenFileCreator tkc;
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
		
	public static void main(String[] args) throws Exception 
	{
		LoaderStep2_TokenFileCreatorTest t = new LoaderStep2_TokenFileCreatorTest();
		
		if ( modes[0].equals(mode) ) 
		{
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) 
		{
	        TestFerrari.testRandom(t);
		} else if  ( modes[2].equals(mode) ) 
		{
			t.setUp();
			t.testCreateTokenFileException();
			t.testCreateTokenFile();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
		tkc = new LoaderStep2_TokenFileCreator();
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testCreateTokenFileException()
	{
		try {
			tkc.createTokenFile(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_dat");
			fail("Should fail with File Exception");
		} catch (Exception e) {
			assertEquals("Improper Exception is thrown", "Parameter 'directory' is not a directory", e.getMessage());
		}
	}
	
	public void testCreateTokenFile() throws Exception
	{
		File sourceDir = new File(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data");
		if(sourceDir.exists())
			sourceDir.delete();
		tkc.createTokenFile(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data");
		File tokenFile = new File(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data" +File.separator +"token_data.tsf");
		assertTrue("Token Files are not created", tokenFile.exists());

		BufferedReader br = new BufferedReader(new FileReader(tokenFile.getAbsolutePath()));
		String currentLine = br.readLine();
		assertNotNull("There is no data in the source file", currentLine);
		assertEquals("Token Data does not match", "-2029849391	september	1	1	1	14.0", currentLine);

	}
}
