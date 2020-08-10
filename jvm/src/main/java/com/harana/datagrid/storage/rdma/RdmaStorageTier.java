package com.harana.datagrid.storage.rdma;

import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageTier;

public class RdmaStorageTier extends RdmaStorageClient implements StorageTier {
	
	public StorageServer launchServer () throws Exception {
		return new RdmaStorageServer();
	}
}