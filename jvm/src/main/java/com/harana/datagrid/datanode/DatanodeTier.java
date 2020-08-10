package com.harana.datagrid.datanode;

import com.harana.datagrid.client.datanode.DatanodeClient;

public interface DatanodeTier extends DatanodeClient {
	
	DatanodeServer launchServer() throws Exception;
	
	@SuppressWarnings("unchecked")
	static DatanodeTier createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (DatanodeTier.class.isAssignableFrom(nodeClass)) {
			Class<? extends DatanodeTier> storageTier = (Class<? extends DatanodeTier>) nodeClass;
			return storageTier.newInstance();
		} else {
			throw new Exception("Cannot instantiate datanode of type " + name);
		}
	}	
}
	