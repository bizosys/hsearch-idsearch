package com.bizosys.hsearch.idsearch.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.FederatedFacade.IRowId;
import com.bizosys.hsearch.federate.QueryArgs;
import com.bizosys.hsearch.idsearch.table.TermQuery;

public class QueryFederator 
{

	public Map<Integer, String> sqlMap = new HashMap<Integer, String>();
	public String federatedQuery = "";
	public Map<String, QueryArgs> queryDetails = new HashMap<String, QueryArgs>();

	public static void main(String[] args) throws Exception 
	{
		QueryFederator qf = new QueryFederator();
		
		String sqlQuery = "SELECT FileReport_Id_n FROM i2iData.dbo.CMR_Study_FileReport AS FR " +
				"INNER JOIN i2iData.dbo.CMR_Study_ReportDetails AS RD ON FR.Report_id_n = RD.Report_Id_n " +
				"INNER JOIN [i2iData].[dbo].[CMR_Study_Info_Mst] AS SIM ON SIM.Study_Unique_Id_n = RD.Study_Id_n " +
				"FULL OUTER JOIN [i2iData].[dbo].CMR_Patient_Info_Mst AS PIM ON SIM.Patient_Id_n = PIM.Patient_Unique_id_n " +
				"WHERE SIM.Patient_Age_n BETWEEN ? AND ?";

		qf.sqlMap.put(23, sqlQuery);
		qf.processQuery("jdbc:q1 OR hsearch:q2 \n jdbc:q1;qid=23,param1=29,param2=30 \n hsearch:q2;brain");

		FederatedSearch federatedSearch = new FederatedSearch();
		federatedSearch.initialize(qf.federatedQuery , qf.queryDetails);
		
		LoaderStep3_ByteFileCreator byteFileC = new LoaderStep3_ByteFileCreator();
		byte[] cellBytes = byteFileC.createByteFile("F:\\Work Documents\\i2i\\reportsbkup\\source_data\\token_data.tsf");
		
		List<FederatedFacade<Long, Long>.IRowId> rows = federatedSearch.execute(cellBytes);
		System.out.println("Number of results received: " +rows.size());
		
		for (IRowId iRowId : rows) {
			System.out.println("Document Id: " +iRowId.getDocId());
		}
	}
	
	//jdbc:q1 OR hsearch:q2 \n jdbc:q1;qid=23,param1=29,param2=30 \n hsearch:q2;brain
	public void processQuery(String queryArgs) throws Exception
	{
		List<String> queries = new ArrayList<String>();
        int pos = 0, end;

        while ((end = queryArgs.indexOf('\n', pos)) >= 0) 
        {
            queries.add(queryArgs.substring(pos, end));
            pos = end + 1;
        }
        queries.add(queryArgs.substring(pos));
        
        Boolean isFirst = true;
		for (String queryString : queries) 
		{
			if(isFirst)
			{
				federatedQuery = queryString.trim();
				isFirst = false;
				continue;
			}
			
			if(queryString.trim().substring(0, 4).equals("jdbc"))
			{
				processSQL(queryString.trim());
			}
			else if(queryString.trim().substring(0, 7).equals("hsearch"))
			{
				processHSearch(queryString.trim());
			}
			else
			{
				throw new IOException("Invalid Query Signature");
			}
		}
	}
	
	//jdbc:q1;qid=23,param1=29,param2=30
	private void processSQL(String query) throws SQLException
	{
        List<String> list = new ArrayList<String>();
        String queryKey;
        int pos = 0, end;
        while ((end = query.indexOf(';', pos)) >= 0) 
        {
            list.add(query.substring(pos, end));
            pos = end + 1;
        }
        list.add(query.substring(pos));
        
    	queryKey = list.get(0);
    	String queryAndParams = list.get(1);
    	pos = 0;
    	list.clear();
        while ((end = queryAndParams.indexOf(',', pos)) >= 0) 
        {
            list.add(queryAndParams.substring(pos, end));
            pos = end + 1;
        }
        list.add(queryAndParams.substring(pos));
    	

        
        String sqlQuery = sqlMap.get(Integer.parseInt(list.get(0).substring(list.get(0).indexOf("=") + 1)));
		QueryArgs queryArgs = new QueryArgs(sqlQuery);
        for (int i = 1; i < list.size(); i++)
        {
        	queryArgs.addParam(list.get(i).substring(list.get(i).indexOf("=") + 1));
        }
		queryDetails.put(queryKey, queryArgs);
	}
	
	//hsearch:q2;brain
	private void processHSearch(String query) throws Exception
	{
		String[] params = query.split(";");
		TermQuery tQuery = new TermQuery();
		tQuery.setField(params[1]);
		queryDetails.put(params[0], new QueryArgs(tQuery.toString()));
	}
}
