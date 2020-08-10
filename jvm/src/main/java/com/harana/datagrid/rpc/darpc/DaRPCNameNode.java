package com.harana.datagrid.rpc.darpc;

import com.harana.datagrid.rpc.RpcBinding;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;
import org.apache.logging.log4j.Logger;

public class DaRPCNameNode extends DaRPCNameNodeClient implements RpcBinding {
	@Override
	public RpcServer launchServer(RpcNameNodeService service) {
		return new DaRPCNameNodeServer(service);
	}

	@Override
	public void printConf(Logger log) {

	}
}