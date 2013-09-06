package com.bizosys.hsearch.kv;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.bizosys.hsearch.byteutils.SortedBytesArray;
import com.bizosys.hsearch.byteutils.SortedBytesInteger;
import com.bizosys.hsearch.kv.impl.ComputeKV;
import com.bizosys.hsearch.kv.impl.Datatype;
import com.bizosys.hsearch.kv.impl.FieldMapping.Field;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository;
import com.bizosys.hsearch.kv.impl.KVDataSchemaRepository.KVDataSchema;
import com.bizosys.hsearch.kv.impl.KVDocIndexer;
import com.bizosys.hsearch.treetable.client.HSearchProcessingInstruction;
import com.bizosys.hsearch.util.HSearchLog;

public class StorageReader implements Callable<Object> {

	public static boolean DEBUG_ENABLED = HSearchLog.l.isDebugEnabled();
	public static boolean INFO_ENABLED = HSearchLog.l.isInfoEnabled();

	public KVDataSchemaRepository repository = null;
	public String schemaRepositoryName = null;
	public KVDocIndexer indexer = null;
	
	public String tableName;
	public String rowId;
	public String filter;
	public int callBackType;	

	public StorageReader(KVDataSchemaRepository repository,
			String schemaRepositoryName, KVDocIndexer indexer,
			String tableName, String rowId, String filter, int callBackType) {
		this.repository = repository;
		this.schemaRepositoryName = schemaRepositoryName;
		this.indexer = indexer;
		this.tableName = tableName;
		this.rowId = rowId;
		this.filter = filter;
		this.callBackType = callBackType;
	}

	@Override
	public Object call() throws Exception {
		Object obk = readStorage();
		return obk;
	}
	
	public final Object readStorage() {
		
		byte[] data = null;
		String fieldName = null;
		KVDataSchema dataScheme = null;
		try {
			
			fieldName = rowId.substring(rowId.lastIndexOf("_") + 1,rowId.length());
			dataScheme = repository.get(schemaRepositoryName);
			Field fld = dataScheme.fm.nameSeqs.get(fieldName);
			int outputType = dataScheme.dataTypeMapping.get(fieldName);
			
			switch (callBackType) {
				case HSearchProcessingInstruction.PLUGIN_CALLBACK_COLS:
				{
					//if the field is not saved it canot be fetched 
					if ( !fld.isSave ) {
						HSearchLog.l.fatal("Field: " + fieldName + " is not saved cannot be selected ");
						throw new IOException("Field: " + fieldName + " is not saved cannot be selected ");
					}

					ComputeKV compute = null;	
					compute = new ComputeKV();
					
					//if docIndex is searched for value then set output type to string
					if(fld.isDocIndex)
						compute.kvType = Datatype.STRING;
					else
						compute.kvType = outputType;
					
					compute.rowContainer = new HashMap<Integer, Object>();
					long start = System.currentTimeMillis();
					data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, compute.kvType);
					compute.put(data);
					if(DEBUG_ENABLED){
						long end = System.currentTimeMillis();
						HSearchLog.l.debug("Hbase Fetch time " + (end - start) +" for " + data.length +" bytes");
					}
					return compute.rowContainer;
				}
				case HSearchProcessingInstruction.PLUGIN_CALLBACK_ID:
				{
					//change filterQuery for documents search
					if(fld.isDocIndex){
						int queryPartLoc = filter.lastIndexOf('|');
						String query = ( queryPartLoc < 0 ) ? filter : filter.substring(queryPartLoc+1);

						String docType = "DOC";
						String fieldType = fieldName;
						
						int fieldTypeLoc = fieldName.indexOf('/');
						if ( fieldTypeLoc > 0 ) {
							docType = fieldName.substring(0, fieldTypeLoc);
							fieldType = fieldName.substring(fieldTypeLoc+1);
						}
						
		    			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Query :[" + query + "] : DocType :[" + docType + "]  : Field Type:[" + fieldType + "]");

		    			Map<String, Integer> dTypes = new HashMap<String, Integer>(1);
		    			dTypes.put(docType, 1);
		    	    	setDocumentTypeCodes(dTypes);
		    			
		    			Map<String, Integer> fTypes= new HashMap<String, Integer>(1);
		    			fTypes.put(fieldType, 1);
		    	    	setFieldTypeCodes(fTypes);
		    	    	
		    			filter = indexer.parseQuery(new StandardAnalyzer(Version.LUCENE_36), docType, fieldType, query);
		    			rowId = rowId + "_I";
		    			
		    			if ( DEBUG_ENABLED ) HSearchLog.l.debug("Document Search => rowId:[" + rowId + "] : Query :[" + filter + "]");
		    		}				
					data = KVRowReader.getAllValues(tableName, rowId.getBytes(), filter, callBackType, outputType);

					Set<Integer> ids = new HashSet<Integer>();
					for (byte[] dataChunk : SortedBytesArray.getInstanceArr().parse(data).values()) {
						SortedBytesInteger.getInstance().parse(dataChunk).values(ids);
						return ids;
					}
					return ids;
				}
			}
		} catch (Exception e) {
			String msg = e.getMessage() + "\nField :" + fieldName + " rowid "+ rowId + " query " + filter;
			if ( null != dataScheme ) {
				if ( null != dataScheme.dataTypeMapping ) {
					msg = msg + "\tdataScheme>" + dataScheme.dataTypeMapping.toString();
				}
			}
			HSearchLog.l.fatal("ReadStorage Exception " + msg , e );
			e.printStackTrace();
		}
		return null;
	}

	
	public final void setFieldTypeCodes(final Map<String, Integer> ftypes) throws IOException {
		this.indexer.addFieldTypes(ftypes);
	}

	public final void setDocumentTypeCodes(final Map<String, Integer> dtypes) throws IOException {
		indexer.addDoumentTypes(dtypes);
	}	
}
