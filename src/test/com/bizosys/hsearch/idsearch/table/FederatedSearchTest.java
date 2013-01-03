package com.bizosys.hsearch.idsearch.table;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.QueryArgs;
import com.bizosys.hsearch.idsearch.client.FederatedSearch;
import com.bizosys.hsearch.idsearch.client.LoaderStep3_ByteFileCreator;
import com.oneline.ferrari.TestAll;

public class FederatedSearchTest extends TestCase 
{

	private FederatedSearch fs;
	private String sqlQuery = "";
	private Map<String, QueryArgs> queryDetails;
	private String query = "";
	
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
		
	public static void main(String[] args) throws Exception 
	{
		FederatedSearchTest t = new FederatedSearchTest();
		
		if ( modes[0].equals(mode) ) 
		{
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) 
		{
	        TestFerrari.testRandom(t);
		} else if  ( modes[2].equals(mode) ) 
		{
			t.setUp();
			t.testInitialize();
			t.testExecute();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
		fs = new FederatedSearch();
		
		sqlQuery = "SELECT FileReport_Id_n FROM i2iData.dbo.CMR_Study_FileReport AS FR " +
				"INNER JOIN i2iData.dbo.CMR_Study_ReportDetails AS RD ON FR.Report_id_n = RD.Report_Id_n " +
				"INNER JOIN [i2iData].[dbo].[CMR_Study_Info_Mst] AS SIM ON SIM.Study_Unique_Id_n = RD.Study_Id_n " +
				"FULL OUTER JOIN [i2iData].[dbo].CMR_Patient_Info_Mst AS PIM ON SIM.Patient_Id_n = PIM.Patient_Unique_id_n " +
				"WHERE SIM.Patient_Age_n BETWEEN ? AND ?";
		
		queryDetails = new HashMap<String, QueryArgs>();

		TermQuery tQuery = new TermQuery();
		tQuery.setField("cuboid");
		
		query = "jdbc:q1 OR hsearch:q2";
		QueryArgs sqlQueryArgs = new QueryArgs(sqlQuery, "29", "30");
		queryDetails.put("jdbc:q1", sqlQueryArgs);
		queryDetails.put("hsearch:q2", new QueryArgs(tQuery.toString()));
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testInitialize() throws Exception
	{
		fs.initialize(query , queryDetails);
		assertEquals("Federated Query Signature mismatch", "jdbc:q1 OR hsearch:q2", fs.getFederatedQuery());
		assertEquals("Wrong number of query arguments", 2, fs.getQueryDetails().size());
		assertEquals("Wrong number of query parameters", 2, (fs.getQueryDetails().get("jdbc:q1")).getParams().size());
		assertEquals("Invalid parameter 1", "29", (fs.getQueryDetails().get("jdbc:q1")).getParams().get(0));
		assertEquals("Invalid parameter 2", "30", (fs.getQueryDetails().get("jdbc:q1")).getParams().get(1));
		assertNull("Search Parameters had to be null", (fs.getQueryDetails().get("hsearch:q2")).getParams());
		assertEquals("Invalid Search Query Signature", "TFF|cuboid|0|0", (fs.getQueryDetails().get("hsearch:q2")).query);
	}

	public void testExecute() throws Exception
	{
		fs.initialize(query , queryDetails);
		LoaderStep3_ByteFileCreator byteFileC = new LoaderStep3_ByteFileCreator();
		byte[] cellBytes = byteFileC.createByteFile("F:" +File.separator +"Work Documents" +File.separator +"i2i" +File.separator +"reportsbkup" +File.separator +"source_data" +File.separator +"token_data.tsf");
		
		List<FederatedFacade<Long, Long>.IRowId> rows = fs.execute(cellBytes);
		assertNotNull("Problem in getting results", rows);
	}
}
