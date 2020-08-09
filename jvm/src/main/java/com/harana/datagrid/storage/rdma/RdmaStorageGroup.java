package com.harana.datagrid.storage.rdma;

import java.io.IOException;

import com.harana.datagrid.metadata.DataNodeInfo;
import com.harana.datagrid.storage.StorageEndpoint;

public interface RdmaStorageGroup {

	public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException;

	public void close() throws InterruptedException, IOException;
	
	public int getType();
}