package com.bizosys.hsearch.idsearch.storage.donotmodify;
import java.io.IOException;
import java.util.Map;
import com.bizosys.hsearch.treetable.client.partition.IPartition;
import com.bizosys.hsearch.treetable.client.partition.PartitionNumeric;
import com.bizosys.hsearch.treetable.client.partition.PartitionByFirstLetter;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaCreator;
import com.bizosys.hsearch.treetable.storage.HBaseTableSchemaDefn;
public class HBaseTableSchema {
	private static HBaseTableSchema singleton = null; 
	public static HBaseTableSchema getInstance() throws IOException {
		if ( null == singleton ) singleton = new HBaseTableSchema();
		return singleton;
	}
	
	private HBaseTableSchema() throws IOException {
		
		HBaseTableSchemaDefn.getInstance().tableName = "hsearch-index";
		Map<String, IPartition> columns = HBaseTableSchemaDefn.getInstance().columnPartions;
		columns.put("Documents",new PartitionByFirstLetter());
		columns.get("Documents").setPartitionsAndRange(
			"Documents",
			/**
			"a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p,q,r,s,t,u,v,w,x,y,z",
			"[a:b],[b:c],[c:d],[d:e],[e:f],[f:g],[g:h],[h:i],[i:j],[j:k],[k:l],[l:m],[m:n],[n:o],[o:p],[p:q],[q:r],[r:s],[s:t],[t:u],[u:v],[v:w],[w:x],[x:y],[y:z],[z:~]",
			*/
			"", "",
			3);
	}
	public HBaseTableSchemaDefn getSchema() {
		return HBaseTableSchemaDefn.getInstance();
	}
	
	public void createSchema() {
		new HBaseTableSchemaCreator().init();
	}
	
	public static void main(String[] args) throws Exception {
		HBaseTableSchema.getInstance().createSchema();
	}
}
