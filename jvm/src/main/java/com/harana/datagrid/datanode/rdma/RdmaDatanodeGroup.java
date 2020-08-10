package com.harana.datagrid.datanode.rdma;

import java.io.IOException;

import com.harana.datagrid.client.datanode.DatanodeEndpoint;
import com.harana.datagrid.metadata.DatanodeInfo;

public interface RdmaDatanodeGroup {

	DatanodeEndpoint createEndpoint(DatanodeInfo info) throws IOException;

	void close() throws InterruptedException, IOException;
	
	int getType();
}