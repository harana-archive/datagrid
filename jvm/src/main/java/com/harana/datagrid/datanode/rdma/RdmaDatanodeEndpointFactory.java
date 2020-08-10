package com.harana.datagrid.datanode.rdma;

import java.io.IOException;

import com.harana.datagrid.rdma.*;
import com.harana.datagrid.rdma.verbs.*;

public class RdmaDatanodeEndpointFactory implements RdmaEndpointFactory<RdmaDatanodeEndpoint> {
	private final RdmaDatanodeServer closer;
	private final RdmaActiveEndpointGroup<RdmaDatanodeEndpoint> group;
	
	public RdmaDatanodeEndpointFactory(RdmaActiveEndpointGroup<RdmaDatanodeEndpoint> group, RdmaDatanodeServer closer) {
		this.group = group;
		this.closer = closer;
	}
	
	@Override
	public RdmaDatanodeEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
		return new RdmaDatanodeEndpoint(group, id, closer, serverSide);
	}
}
