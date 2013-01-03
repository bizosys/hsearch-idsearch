package com.bizosys.hsearch.idsearch.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.thirdparty.guava.common.io.Files;

import com.bizosys.hsearch.idsearch.table.ITermTable;
import com.bizosys.hsearch.idsearch.table.TermTableFactory;
import com.bizosys.hsearch.idsearch.table.TermTableRow;

public class LoaderStep3_ByteFileCreator 
{
	public byte[] createByteFile(String filePath) throws Exception
	{
		ITermTable termTable = TermTableFactory.getTable();
		File searchFile = new File(filePath);
		BufferedReader reader = null;
		InputStream stream = null;
		byte[] cellBytes = null;
		
		System.out.println("FILE READING");
		stream = new FileInputStream(searchFile); 
		reader = new BufferedReader ( new InputStreamReader(stream) );
		
		String line;
		TermTableRow searchData;
		while((line=reader.readLine())!=null)
		{
			String[] lineVars = line.split("\t");
			searchData = new TermTableRow();
			searchData.setParams(lineVars[1], Integer.parseInt(lineVars[2]), Integer.parseInt(lineVars[3]), 
					Integer.parseInt(lineVars[4]), Float.parseFloat(lineVars[5]));
			termTable.addSearchData(searchData);
		}
		cellBytes = termTable.toBytes();
		System.out.println("FILE WRITING START");
		File byteFile = new File(filePath.substring(0, filePath.lastIndexOf(File.separator)) +File.separator +"token.ser");
		Files.write(cellBytes, byteFile);
		System.out.println("FILE WRITING DONE");

		return cellBytes;
	}

	public static void main(String[] args) throws Exception
	{
		LoaderStep3_ByteFileCreator byc = new LoaderStep3_ByteFileCreator();
		byc.createByteFile(File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data" +File.separator +"token_data.tsf");
	}
}
