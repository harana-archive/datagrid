package com.harana.datagrid.datanode.tcp;

import com.harana.datagrid.datanode.DatanodeServer;
import com.harana.datagrid.datanode.DatanodeTier;
import org.apache.logging.log4j.Logger;

public class TcpDatagridTier extends TcpDatanodeClient implements DatanodeTier {

	public DatanodeServer launchServer () {
		return new TcpDatanodeServer();
	}

	@Override
	public void printConf(Logger log) {}
}