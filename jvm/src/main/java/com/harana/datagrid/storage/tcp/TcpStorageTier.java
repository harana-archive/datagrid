package com.harana.datagrid.storage.tcp;

import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageTier;
import org.apache.logging.log4j.Logger;

public class TcpStorageTier extends TcpStorageClient implements StorageTier {

	public StorageServer launchServer () {
		return new TcpStorageServer();
	}

	@Override
	public void printConf(Logger log) {}
}