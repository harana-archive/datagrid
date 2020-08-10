package com.harana.datagrid.rpc;

public interface RpcBinding extends RpcClient {
	RpcServer launchServer(RpcNameNodeService service);
	
	@SuppressWarnings("unchecked")
	static RpcBinding createInstance(String name) throws Exception {
		Class<?> nodeClass = Class.forName(name);
		if (RpcBinding.class.isAssignableFrom(nodeClass)){
			Class<? extends RpcBinding> bindingClass = (Class<? extends RpcBinding>) nodeClass;
			return bindingClass.newInstance();
		} else {
			throw new Exception("Cannot instantiate datanode of type " + name);
		}
	}		
}