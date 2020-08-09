package com.harana.datagrid.rpc;

import com.harana.datagrid.conf.Configurable;

public abstract class RpcServer implements Configurable {
	public abstract void run();
}
