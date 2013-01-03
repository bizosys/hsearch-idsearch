package com.bizosys.hsearch.idsearch.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import com.bizosys.hsearch.idsearch.meta.DocMetaFilters;
import com.bizosys.hsearch.idsearch.meta.DocMetaTableRow;
import com.bizosys.hsearch.idsearch.table.TermTable;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.Cell3;
import com.bizosys.hsearch.treetable.Cell4;
import com.bizosys.hsearch.treetable.Cell5;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.bizosys.hsearch.unstructured.tokenizer.ComputeTokens;
import com.bizosys.hsearch.unstructured.tokenizer.FilterStem;
import com.bizosys.hsearch.unstructured.tokenizer.PipeInData;
import com.bizosys.hsearch.unstructured.tokenizer.TokenizeStandard;
import com.bizosys.hsearch.unstructured.util.ContentField;
import com.bizosys.hsearch.unstructured.util.Document;
import com.oneline.ApplicationFault;
import com.oneline.SystemFault;

public class LoaderStep2_TokenFileCreator 
{
	public void createTokenFile(String docDir) throws ApplicationFault, SystemFault, IOException 
	{
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		Iterator<File> iter = FileUtils.iterateFiles(new File(docDir), new String[]{"tsf"}, true);
		while(iter.hasNext()) 
		{
		    File file = (File) iter.next();
		    String fileAbsolutePath = file.getAbsolutePath();
		    
			try 
			{
				String sCurrentLine;
				br = new BufferedReader(new FileReader(fileAbsolutePath));

				while ((sCurrentLine = br.readLine()) != null) 
				{
					sb.append(sCurrentLine);
				}
			} 
			catch (IOException e) 
			{
				e.printStackTrace();
			} 
			finally 
			{
				try 
				{
					if (br != null)
						br.close();
				} 
				catch (IOException ex) 
				{
					ex.printStackTrace();
				}
			}
		}
		
		ContentField fld = new ContentField();
		fld.name = 1;
		fld.content = sb.toString();
		fld.searchable = true;
		fld.keepOriginal = false;
		fld.weight = 11;

		PipeInData data = new PipeInData();
		data.processingDoc = new Document();
		data.processingDoc.meta = new DocMetaTableRow();
		data.processingDoc.meta.docId = 1;
		data.processingDoc.meta.filters = new DocMetaFilters( (byte) 0, 1, 1L, 1L) ;
		data.processingDoc.fields = new ArrayList<ContentField>();
		data.processingDoc.fields.add(fld);
		
		data.termsFromAllDocuments = new TermTable();

		System.out.println( "TokenizeStandard. START" );
		new TokenizeStandard().visit(data);
		System.out.println( "TokenizeStandard. END Total Streams :" + data.processingDocTokenStreams.size());
		
		new FilterStem().visit(data);
		new ComputeTokens().visit(data);
		
		
		Map<Integer, Cell5<String, Integer, Integer, Integer, Float>> mapHashCodecs = data.termsFromAllDocuments.getTable().getMap();
		
		StringBuilder outputData = new StringBuilder();
		Iterator<Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>>> hascodecItr = mapHashCodecs.entrySet().iterator();  
		while ( hascodecItr.hasNext() ) {
			Entry<Integer, Cell5<String, Integer, Integer, Integer, Float>> aHash = hascodecItr.next();
			Integer _hashCode = aHash.getKey();
			Cell5<String, Integer, Integer, Integer, Float> cell5 = aHash.getValue();
			Iterator<Entry<String, Cell4<Integer, Integer, Integer, Float>>> itemItr = 
					cell5.getMap().entrySet().iterator();  
			while ( itemItr.hasNext()) {
				Entry<String, Cell4<Integer, Integer, Integer, Float>> aTerm = itemItr.next();
				String _term = aTerm.getKey();
				Cell4<Integer, Integer, Integer, Float> cell4 = aTerm.getValue();
				Iterator<Entry<Integer, Cell3<Integer, Integer, Float>>> docItr = cell4.getMap().entrySet().iterator();
				while ( docItr.hasNext()) {

					Entry<Integer, Cell3<Integer, Integer, Float>> aDoc = docItr.next();
					Integer _doc = aDoc.getKey();
					Cell3<Integer, Integer, Float> cell3 = aDoc.getValue();
					
					Iterator<Entry<Integer, Cell2<Integer, Float>>> termtypeItr = cell3.getMap().entrySet().iterator();
					while ( termtypeItr.hasNext()) {
					
						Entry<Integer, Cell2<Integer, Float>> word = termtypeItr.next();
						Integer _wordtype = word.getKey();
						Cell2<Integer, Float> cell2 = word.getValue();
						
						for (CellKeyValue<Integer, Float> _word : cell2.getMap()) {
							outputData.append(_hashCode + "\t" + _term + "\t" + _doc + "\t" + _wordtype + "\t" + _word.getKey() + "\t" + _word.getValue() + "\n");
						}
					}
				}
				
			}
		}
		writeToFile(outputData.toString(), docDir);
	}
	
	private static void writeToFile(String fileData, String filePath)
	{
		try
		{
			File tsfFile = new File(filePath +File.separator +"token_data.tsf");
			if(!tsfFile.exists())
			{
				System.out.println("Creating file");
				tsfFile.createNewFile();
			}
			FileWriter fstream = new FileWriter(tsfFile, true);
			
			BufferedWriter out = new BufferedWriter(fstream);
			System.out.println("Writing to file");
			out.write(fileData);
			out.close();
			fstream.close();
			System.out.println("Done with writing");
		}
		catch (Exception e)
		{
			System.err.println("Error: " + e.getMessage());
		}	
	}

//	public static void main(String[] args) throws Exception 
//	{
//		LoaderStep2_TokenFileCreator tkc = new LoaderStep2_TokenFileCreator();
//		tkc.createTokenFile("F:" +File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data");
//	}
}
