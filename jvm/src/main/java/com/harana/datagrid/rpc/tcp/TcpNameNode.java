package com.harana.datagrid.namenode.rpc.tcp;

import com.harana.datagrid.rpc.RpcBinding;
import com.harana.datagrid.rpc.RpcNameNodeService;
import com.harana.datagrid.rpc.RpcServer;

public class TcpNameNode extends TcpNameNodeClient implements RpcBinding {

	@Override
	public RpcServer launchServer(RpcNameNodeService service) {
		try {
			return new TcpNameNodeServer(service);
		} catch(Exception e){
			return null;
		}
	}

}
