package com.bizosys.hsearch.federated;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bizosys.hsearch.federate.FederatedFacade;
import com.bizosys.hsearch.federate.QueryArgs;
import com.bizosys.hsearch.idsearch.table.ITermTable;
import com.bizosys.hsearch.idsearch.table.TermQuery;
import com.bizosys.hsearch.idsearch.table.TermTableFactory;
import com.bizosys.hsearch.treetable.Cell2;
import com.bizosys.hsearch.treetable.CellKeyValue;

public class FederatedSearch 
{
	byte[] col1Val = null;
	byte[] col2Val = null;
	
	public static void main(String[] args) throws Exception 
	{
		Map<String, QueryArgs> queryDetails = new HashMap<String, QueryArgs>();

		TermQuery tQuery1 = new TermQuery();
		tQuery1.setWordRecordtypeFieldType("searchField1", 1, 1);
		
		TermQuery tQuery2 = new TermQuery();
		tQuery2.setWordRecordtypeFieldType("searchField2", 1, 2);

		queryDetails.put("idsearch:tQuery1", new QueryArgs(tQuery1.toString()));
		queryDetails.put("idsearch:tQuery2", new QueryArgs(tQuery2.toString()));
		
		FederatedSearch federatedSearch = new FederatedSearch();
		federatedSearch.initialize("idsearch:tQuery1 OR idsearch:tQuery2" , queryDetails);
		Map<String, ITermTable> searchIds = new HashMap<String, ITermTable>();
		ITermTable termTable1 = TermTableFactory.getTable();
		ITermTable termTable2 = TermTableFactory.getTable();
		
		searchIds.put("tt1", termTable1);
		searchIds.put("tt2", termTable2);
		
		for (ITermTable termTable : searchIds.values()) 
		{
			List<FederatedFacade<Long, Long>.IRowId> rows = federatedSearch.execute(termTable.toBytes());
			System.out.println(rows.toString());
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
					
					Cell2<Integer, Float> out;
					try 
					{
						out = termTable.findIdsFromSerializedTableQuery(cellBytes, termFilter);
					} 
					catch (Exception e) 
					{
						e.printStackTrace(System.err);
						throw new IOException("Corrupted Cube");
					}
	
					List<com.bizosys.hsearch.federate.FederatedFacade<Long, Long>.IRowId> results = 
							new ArrayList<com.bizosys.hsearch.federate.FederatedFacade<Long, Long>.IRowId>(out.getMap().size());
					
					for (CellKeyValue<Integer, Float> _word : out.getMap()) 
					{
						IRowId primary = objectFactory.getPrimaryKeyRowId(_word.getKey().longValue());
						results.add(primary);
					}
					
					return results;
				}
				
				HQuery hquery = null;
				@Override
				public final List<IRowId> execute(String query, Map<String, QueryArgs> queryArgs) throws Exception 
				{
					if ( null == hquery) 
					{
						this.hquery = new HQueryParser().parse(query);
					}
					
					List<HTerm> terms = new ArrayList<HTerm>();
					new HQuery().toTerms(hquery, terms);
					
					for (HTerm aTerm : terms) 
					{
						HResult result = new HResult();
						QueryArgs qa = queryArgs.get(aTerm.type + ":" + aTerm.text);
	
						result.setRowIds(populate(aTerm.type, aTerm.text, qa.query, null));
						aTerm.setResult(result);
					}
	
					List<IRowId> finalResult = new ArrayList<IRowId>(4096);
					new HQueryCombiner().combine(hquery, finalResult);
					
					return finalResult;
				}			
			};
	}
}
