package com.harana.datagrid.storage;

import com.harana.datagrid.storage.StorageClient;

public interface StorageTier extends StorageClient {
	
	public abstract StorageServer launchServer() throws Exception;
	
	@SuppressWarnings("unchecked")
	public static StorageTier createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (StorageTier.class.isAssignableFrom(nodeClass)){
			Class<? extends StorageTier> storageTier = (Class<? extends StorageTier>) nodeClass;
			StorageTier tier = storageTier.newInstance();
			return tier;
		} else {
			throw new Exception("Cannot instantiate datanode of type " + name);
		}
	}	
}
	