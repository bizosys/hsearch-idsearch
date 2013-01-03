package com.bizosys.hsearch.idsearch.table;

import java.io.File;
import java.io.FileInputStream;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.idsearch.client.LoaderStep3_ByteFileCreator;
import com.oneline.ferrari.TestAll;

public class LoaderStep3_ByteFileCreatorTest extends TestCase 
{

	private LoaderStep3_ByteFileCreator bfc;
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
		
	public static void main(String[] args) throws Exception 
	{
		LoaderStep3_ByteFileCreatorTest t = new LoaderStep3_ByteFileCreatorTest();
		
		if ( modes[0].equals(mode) ) 
		{
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) 
		{
	        TestFerrari.testRandom(t);
		} else if  ( modes[2].equals(mode) ) 
		{
			t.setUp();
			t.testCreateByteFileException();
			t.testCreateByteFile();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
		bfc = new LoaderStep3_ByteFileCreator();
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testCreateByteFileException()
	{
		try {
			bfc.createByteFile(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_dat");
			fail("Should fail with File Exception");
		} catch (Exception e) {
			assertEquals("Improper Exception is thrown", File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_dat (The system cannot find the file specified)", e.getMessage());
		}
	}
	
	public void testCreateByteFile() throws Exception
	{
		bfc.createByteFile(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data" +File.separator +"token_data.tsf");
		File byteFile = new File(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data" +File.separator +"token.ser");
		assertTrue("Byte File is not created", byteFile.exists());

		FileInputStream fin = new FileInputStream(byteFile);
		byte fileContent[] = new byte[(int)byteFile.length()];
		fin.read(fileContent);
		String strFileContent = new String(fileContent);
		assertNotNull("There is no data in the file", strFileContent);
	}
}
