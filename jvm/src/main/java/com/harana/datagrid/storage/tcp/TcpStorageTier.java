package com.harana.datagrid.storage.tcp;

import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageTier;

public class TcpStorageTier extends TcpStorageClient implements StorageTier {
	public StorageServer launchServer () throws Exception {
		TcpStorageServer datanodeServer = new TcpStorageServer();
		return datanodeServer;
	}
}
