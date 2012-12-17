package com.bizosys.hsearch.idsearch.client;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.bizosys.hsearch.idsearch.config.AccessTypeCodes;
import com.bizosys.hsearch.idsearch.config.AccessValueCodes;
import com.bizosys.hsearch.idsearch.meta.DocMetaAccess;

public class DocMetaAccessClient {
	public DocMetaAccess access = null;
	
	public DocMetaAccessClient() {
		access = new DocMetaAccess();
	}
	
	public DocMetaAccessClient(DocMetaAccess access) {
		this.access = access;
	}
	
	public void setAccess(Map<String, String> typesAndValues) throws IOException, InstantiationException {
		
		Map<Integer, Integer> typesAndValueCodes = new HashMap<Integer, Integer>(typesAndValues.size());
		for (String accessType: typesAndValues.keySet()) {
			int accessTypeCode = AccessTypeCodes.getInstance().getCode(accessType);
			int accessValueCode = AccessValueCodes.getInstance().getCode(typesAndValues.get(accessType));
			typesAndValueCodes.put(accessTypeCode, accessValueCode);
		}
		access.setAccess(typesAndValueCodes);
	}

	public void setAccess(String accessTypeSuchAsRole, String accessValueSuchAsArchitect) throws IOException, InstantiationException {
		int accessTypeCode = AccessTypeCodes.getInstance().getCode(accessTypeSuchAsRole);
		int accessValueCode = AccessValueCodes.getInstance().getCode(accessValueSuchAsArchitect);
		access.setAccess(accessTypeCode, accessValueCode);
	}
	
	public boolean hasAccess(Map<String, String> userAccessTypeAndValues) throws IOException, InstantiationException {
		Set<Integer> tempMatchingAccessTypes = new HashSet<Integer>();
		return hasAccess(userAccessTypeAndValues, tempMatchingAccessTypes);
	}
	
	public boolean hasAccess(Map<String, String> userAccessTypeAndValues, 
			Set<Integer> tempMatchingAccessTypes) throws IOException, InstantiationException {
		
		Map<Integer, Integer> userAccessTypeAndValuesCodes = 
			new HashMap<Integer, Integer>(userAccessTypeAndValues.size());
		
		for (String aProfileType : userAccessTypeAndValues.keySet()) {

			if ( ! AccessTypeCodes.getInstance().hasCode(aProfileType)) {
				System.err.println ( "Warning : The access type is not found.");
				continue;
			}
			
			String aProfileValue = userAccessTypeAndValues.get(aProfileType);
			if ( ! AccessValueCodes.getInstance().hasCode(aProfileValue)) {
				System.err.println ( "Warning : The access value is not found.");
				continue;
			}
			
			int keyCode = AccessTypeCodes.getInstance().getCode(aProfileType);
			int valCode = AccessValueCodes.getInstance().getCode(aProfileValue);
			userAccessTypeAndValuesCodes.put(keyCode, valCode);
		}
		return 	this.access.hasAccess(userAccessTypeAndValuesCodes);
	}
	
	public static void main(String[] args) throws Exception {
		
		Map<String, Integer> accessTypes = new HashMap<String, Integer>();
		accessTypes.put("role", 1);
		accessTypes.put("uid", 2);
		accessTypes.put("dc", 3);
		accessTypes.put("team", 4);
		
		Map<String, Integer> accessValues = new HashMap<String, Integer>();
		accessValues.put("developer", 1);
		accessValues.put("architect", 2);
		accessValues.put("N4501", 3);
		accessValues.put("com.bizosys.bangalore", 4);
		accessValues.put("team1", 5);
		accessValues.put("team2", 6);

		
		AccessTypeCodes.instanciate(AccessTypeCodes.builder().add(accessTypes).toBytes());
		AccessValueCodes.instanciate(AccessValueCodes.builder().add(accessValues).toBytes());
		

		DocMetaAccessClient serAccess = new DocMetaAccessClient();
		serAccess.setAccess("role", "developer");
//		serAccess.setAccess("role", "architect");
//		serAccess.setAccess("uid", "N4501");
//		serAccess.setAccess("team", "team1");
//		serAccess.setAccess("team", "team2");
//		serAccess.setAccess("dc", "com.bizosys.bangalore");

		Map<String, String> queryUser = new HashMap<String, String>();
		queryUser.put("role", "developer");
		
		DocMetaAccessClient deserAccess = null;
		long start = System.currentTimeMillis();
		deserAccess =  new DocMetaAccessClient ( new DocMetaAccess(serAccess.access.toBytes()) );

		Set<Integer> tempMatchingAccessTypes = new HashSet<Integer>();
		for ( int i=0; i<1000000; i++) {
			deserAccess.hasAccess(queryUser, tempMatchingAccessTypes);
		}
		long end = System.currentTimeMillis();
		System.out.println ( deserAccess.hasAccess(queryUser) + "   in " + (end - start) );
	}
}
