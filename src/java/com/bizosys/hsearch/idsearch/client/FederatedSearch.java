package com.bizosys.hsearch.idsearch.client;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.byteutils.SortedBytesFloat;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.FederatedFacade.IRowId;
import com.bizosys.hsearch.federate.QueryArgs;
import com.bizosys.hsearch.idsearch.table.ITermTable;
import com.bizosys.hsearch.idsearch.table.TermQuery;
import com.bizosys.hsearch.idsearch.table.TermTable;
import com.bizosys.hsearch.idsearch.table.TermTableFactory;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellKeyValue;
import com.oneline.dao.PoolFactory;
import com.oneline.dao.ReadScalar;
import com.oneline.util.FileReaderUtil;

public class FederatedSearch 
{
	public static void main(String[] args) throws Exception 
	{
		Map<String, QueryArgs> queryDetails = new HashMap<String, QueryArgs>();

		TermQuery tQuery = new TermQuery();
		tQuery.setField("cuboid");
		
		String sqlQuery = "SELECT FileReport_Id_n FROM i2iData.dbo.CMR_Study_FileReport AS FR " +
				"INNER JOIN i2iData.dbo.CMR_Study_ReportDetails AS RD ON FR.Report_id_n = RD.Report_Id_n " +
				"INNER JOIN [i2iData].[dbo].[CMR_Study_Info_Mst] AS SIM ON SIM.Study_Unique_Id_n = RD.Study_Id_n " +
				"FULL OUTER JOIN [i2iData].[dbo].CMR_Patient_Info_Mst AS PIM ON SIM.Patient_Id_n = PIM.Patient_Unique_id_n " +
				"WHERE SIM.Patient_Age_n BETWEEN ? AND ?";
		
		String query = "jdbc:q1 AND hsearch:q2";
		QueryArgs sqlQueryArgs = new QueryArgs(sqlQuery, "29", "30");
		queryDetails.put("jdbc:q1", sqlQueryArgs);
		queryDetails.put("hsearch:q2", new QueryArgs(tQuery.toString()));
		
		FederatedSearch federatedSearch = new FederatedSearch();
		federatedSearch.initialize(query , queryDetails);

		LoaderStep3_ByteFileCreator byteFileC = new LoaderStep3_ByteFileCreator();
		byte[] cellBytes = byteFileC.createByteFile("F:\\Work Documents\\i2i\\reportsbkup\\source_data\\token_data.tsf");
		
		List<FederatedFacade<Long, Long>.IRowId> rows = federatedSearch.execute(cellBytes);
		System.out.println("Number of results received: " +rows.size());
		
		for (IRowId iRowId : rows) {
			System.out.println("Document Id: " +iRowId.getDocId());
		}
	}
	
	FederatedFacade<Long, Long> facade = null;
	Map<String, QueryArgs> queryDetails = null;
	String federatedQuery = null;
	public byte[] cellBytes = null;
	
	public void initialize(String federatedQuery, Map<String, QueryArgs> queryDetails) 
	{
		this.facade = createFacade(); 
		this.queryDetails = queryDetails;
		this.federatedQuery = federatedQuery;
	}
	
	public List<FederatedFacade<Long, Long>.IRowId> execute(byte[] cellBytes) throws Exception 
	{
		this.cellBytes = cellBytes;
		List<FederatedFacade<Long, Long>.IRowId> finalResult = facade.execute(federatedQuery, queryDetails);
		return finalResult;
	}

	/**
	 * Each query comes with type as the colFamily_col and queryId the value
	 * If QueryId is null, that means it is range query. Look for minimum and maximum
	 * 
	 * @return
	 */
	private FederatedFacade<Long, Long> createFacade() 
	{
		return new FederatedFacade<Long, Long>(-1L, 30000, 2) 
			{
				Map<String, TermQuery> termFilter = new HashMap<String, TermQuery>();
				ITermTable termTable = TermTableFactory.getTable();
				
				@Override
				public List<com.bizosys.hsearch.federate.FederatedFacade<Long, Long>.IRowId> populate(
						String type, String queryId, String queryDetail, List<String> params) throws IOException 
				{
					List<com.bizosys.hsearch.federate.FederatedFacade<Long, Long>.IRowId> results = 
							new ArrayList<com.bizosys.hsearch.federate.FederatedFacade<Long, Long>.IRowId>();
					IRowId primary;
					if(type.equals("jdbc"))
					{
						System.out.println("Query Detail: " +queryDetail);
						
						int i = 0;
						for (String param : params) 
						{
							System.out.println("Param" +i +": " +param);
							queryDetail = queryDetail.replaceFirst("\\?", param);
							i++;
						}
				        System.out.println(queryDetail);
				        PoolFactory.getInstance().setup(FileReaderUtil.toString("db.conf"));
				        List<Object> ids = new ArrayList<Object>();
				        try 
				        {
							ids = new ReadScalar().execute(queryDetail, ids);
						} 
				        catch (SQLException e) 
				        {
							e.printStackTrace();
						}
				        
				        for (Object resultObj : ids) 
				        {
							primary = objectFactory.getPrimaryKeyRowId((long)Integer.parseInt(resultObj.toString()));
							results.add(primary);
						}
					}
					else if(type.equals("hsearch"))
					{
						TermQuery tQuery = null;
						if ( termFilter.containsKey(queryDetail) ) 
						{
							tQuery = termFilter.get(queryDetail);
						} 
						else 
						{
							tQuery = new TermQuery(queryDetail);
							termFilter.put(queryDetail, tQuery);
						}
						
						Cell2<Integer, Float> out = new Cell2<Integer, Float>(SortedBytesInteger.getInstance(), SortedBytesFloat.getInstance());;
						try 
						{
							out = ((TermTable)termTable).findIdsFromSerializedTableQuery(cellBytes, termFilter);
						} 
						catch (Exception e) 
						{
							e.printStackTrace(System.err);
							throw new IOException("Corrupted Cube");
						}
		
						for (CellKeyValue<Integer, Float> _word : out.getMap()) 
						{
							primary = objectFactory.getPrimaryKeyRowId(_word.getKey().longValue());
							results.add(primary);
						}
					}
					else
						System.out.println("Invalid Query Type");
					
					
					return results;
				}
			};
	}

	public Map<String, QueryArgs> getQueryDetails() {
		return queryDetails;
	}

	public String getFederatedQuery() {
		return federatedQuery;
	}
}
