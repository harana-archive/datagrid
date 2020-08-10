package com.harana.datagrid.rpc.rdma;

import com.harana.datagrid.rpc.RpcBinding;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import org.apache.logging.log4j.Logger;

public class RdmaNameNode extends RdmaNameNodeClient implements RpcBinding {
	@Override
	public RpcServer launchServer(RpcNameNodeService service) {
		return new RdmaNameNodeServer(service);
	}

	@Override
	public void printConf(Logger log) {

	}
}