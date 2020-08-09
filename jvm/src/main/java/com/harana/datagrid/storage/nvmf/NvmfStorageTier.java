package com.harana.datagrid.storage.nvmf;

import com.harana.datagrid.storage.StorageServer;
import com.harana.datagrid.storage.StorageTier;
import com.harana.datagrid.utils.CrailUtils;
import org.slf4j.Logger;

public class NvmfStorageTier extends NvmfStorageClient implements StorageTier {
	private static final Logger logger = LogManager.getLogger();

	public StorageServer launchServer() throws Exception {
		logger.info("initalizing NVMf storage tier");
		NvmfStorageServer storageServer = new NvmfStorageServer();
		return storageServer;
	}
}
