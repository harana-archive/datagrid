package com.harana.datagrid.datanode.rdma;

import com.harana.datagrid.datanode.DatanodeServer;
import com.harana.datagrid.datanode.DatanodeTier;

public class RdmaDatanodeTier extends RdmaDatanodeClient implements DatanodeTier {
	
	public DatanodeServer launchServer () throws Exception {
		return new RdmaDatanodeServer();
	}
}