package com.bizosys.hsearch.idsearch.table;

import junit.framework.TestCase;
import junit.framework.TestFerrari;

import com.bizosys.hsearch.idsearch.client.QueryFederator;
import com.oneline.ferrari.TestAll;

public class QueryFederatorTest extends TestCase 
{

	private QueryFederator qf;
	public static String[] modes = new String[] { "all", "random", "method"};
	public static String mode = modes[2];  
		
	public static void main(String[] args) throws Exception 
	{
		QueryFederatorTest t = new QueryFederatorTest();
		
		if ( modes[0].equals(mode) ) 
		{
			TestAll.run(new TestCase[]{t});
		} else if  ( modes[1].equals(mode) ) 
		{
	        TestFerrari.testRandom(t);
		} else if  ( modes[2].equals(mode) ) 
		{
			t.setUp();
			t.testProcessQueryException1();
			t.testProcessQueryException2();
			t.testProcessQueryOnlyJDBC();
			t.testProcessQueryOnlyHSearch();
			t.testProcessQueryFederated();
			t.tearDown();
		}
	}

	@Override
	protected void setUp() throws Exception {
		qf = new QueryFederator();
		
		String sqlQuery1 = "SELECT FileReport_Id_n FROM i2iData.dbo.CMR_Study_FileReport AS FR " +
				"INNER JOIN i2iData.dbo.CMR_Study_ReportDetails AS RD ON FR.Report_id_n = RD.Report_Id_n " +
				"INNER JOIN [i2iData].[dbo].[CMR_Study_Info_Mst] AS SIM ON SIM.Study_Unique_Id_n = RD.Study_Id_n " +
				"FULL OUTER JOIN [i2iData].[dbo].CMR_Patient_Info_Mst AS PIM ON SIM.Patient_Id_n = PIM.Patient_Unique_id_n " +
				"WHERE SIM.Patient_Age_n BETWEEN ? AND ?";

		String sqlQuery2 = "SELECT FileReport_Id_n FROM i2iData.dbo.CMR_Study_FileReport AS FR " +
				"INNER JOIN i2iData.dbo.CMR_Study_ReportDetails AS RD ON FR.Report_id_n = RD.Report_Id_n " +
				"INNER JOIN [i2iData].[dbo].[CMR_Study_Info_Mst] AS SIM ON SIM.Study_Unique_Id_n = RD.Study_Id_n " +
				"WHERE SIM.Study_Unique_Id_n = ?";

		String sqlQuery3 = "SELECT FileReport_Id_n FROM i2iData.dbo.CMR_Study_FileReport AS FR " +
				"INNER JOIN i2iData.dbo.CMR_Study_ReportDetails AS RD ON FR.Report_id_n = RD.Report_Id_n " +
				"INNER JOIN [i2iData].[dbo].[CMR_Study_Info_Mst] AS SIM ON SIM.Study_Unique_Id_n = RD.Study_Id_n " +
				"WHERE SIM.Inst_Id_n = ?";
		
		qf.sqlMap.put(1, sqlQuery1);
		qf.sqlMap.put(2, sqlQuery2);
		qf.sqlMap.put(3, sqlQuery3);
	}
	
	@Override
	protected void tearDown() throws Exception {
	}
	
	public void testProcessQueryException1()
	{
		try 
		{
			qf.processQuery("jdbc:q1 OR hsearch:q2 \n sql:q1;qid=23,param1=29,param2=30 \n hsearch:q2;brain");
			fail("Should fail with IO Exception");
		} 
		catch (Exception e) 
		{
			assertEquals("Improper Exception is thrown", "Invalid Query Signature", e.getMessage());
		}
	}
	
	public void testProcessQueryException2()
	{
		try 
		{
			qf.processQuery("jdbc:q1 OR hsearch:q2 \n jdbc:q1;qid=23,param1=29,param2=30 \n search:q2;brain");
			fail("Should fail with IO Exception");
		} 
		catch (Exception e) 
		{
			assertEquals("Improper Exception is thrown", "Invalid Query Signature", e.getMessage());
		}
	}
	
	public void testProcessQueryOnlyJDBC() throws Exception
	{
		qf.processQuery("jdbc:q1 \n jdbc:q1;qid=1,param1=29,param2=30");
		assertEquals("Federated Query Signature mismatch", "jdbc:q1", qf.federatedQuery);
		assertEquals("Wrong number of query arguments", 1, qf.queryDetails.size());
		assertEquals("Wrong number of query parameters", 2, (qf.queryDetails.get("jdbc:q1")).getParams().size());
		assertEquals("Invalid parameter 1", "29", (qf.queryDetails.get("jdbc:q1")).getParams().get(0));
		assertEquals("Invalid parameter 2", "30", (qf.queryDetails.get("jdbc:q1")).getParams().get(1));
	}

	public void testProcessQueryOnlyHSearch() throws Exception
	{
		qf.processQuery("hsearch:q1 \n hsearch:q1;brain");
		assertEquals("Federated Query Signature mismatch", "hsearch:q1", qf.federatedQuery);
		assertEquals("Wrong number of query arguments", 1, qf.queryDetails.size());
		assertNull("Parameters had to be null", (qf.queryDetails.get("hsearch:q1")).getParams());
		assertEquals("Invalid parameter 1", "TFF|brain|0|0", (qf.queryDetails.get("hsearch:q1")).query);
	}

	public void testProcessQueryFederated() throws Exception
	{
		qf.processQuery("jdbc:q1 OR hsearch:q2 \n jdbc:q1;qid=23,param1=29,param2=30 \n hsearch:q2;brain");
		assertEquals("Federated Query Signature mismatch", "jdbc:q1 OR hsearch:q2", qf.federatedQuery);
		assertEquals("Wrong number of query arguments", 2, qf.queryDetails.size());
		assertEquals("Wrong number of sql query parameters", 2, (qf.queryDetails.get("jdbc:q1")).getParams().size());
		assertEquals("Invalid parameter 1", "29", (qf.queryDetails.get("jdbc:q1")).getParams().get(0));
		assertEquals("Invalid parameter 2", "30", (qf.queryDetails.get("jdbc:q1")).getParams().get(1));
		assertNull("Search Parameters had to be null", (qf.queryDetails.get("hsearch:q2")).getParams());
		assertEquals("Invalid Search Query Signature", "TFF|brain|0|0", (qf.queryDetails.get("hsearch:q2")).query);
	}
}
