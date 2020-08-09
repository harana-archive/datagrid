package com.harana.datagrid.namenode.rpc.darpc;

import com.harana.datagrid.rpc.RpcBinding;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;

public class DaRPCNameNode extends DaRPCNameNodeClient implements RpcBinding {
	@Override
	public RpcServer launchServer(RpcNameNodeService service) {
		return new DaRPCNameNodeServer(service);
	}
}
