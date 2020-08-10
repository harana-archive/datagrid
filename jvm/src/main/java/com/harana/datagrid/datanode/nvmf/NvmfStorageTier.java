package com.harana.datagrid.datanode.nvmf;

import com.harana.datagrid.datanode.DatanodeServer;
import com.harana.datagrid.datanode.DatanodeTier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NvmfStorageTier extends NvmfDatanodeClient implements DatanodeTier {
	private static final Logger logger = LogManager.getLogger();

	public DatanodeServer launchServer() throws Exception {
		logger.info("initalizing NVMf storage tier");
		NvmfStorageServer storageServer = new NvmfStorageServer();
		return storageServer;
	}
}
