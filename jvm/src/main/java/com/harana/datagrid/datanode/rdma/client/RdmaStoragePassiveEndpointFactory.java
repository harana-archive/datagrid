package com.harana.datagrid.datanode.rdma.client;

import java.io.IOException;
import com.harana.datagrid.rdma.verbs.*;
import com.harana.datagrid.rdma.*;

public class RdmaStoragePassiveEndpointFactory implements RdmaEndpointFactory<RdmaStoragePassiveEndpoint> {
	private final RdmaStoragePassiveGroup group;
	
	public RdmaStoragePassiveEndpointFactory(RdmaStoragePassiveGroup group) {
		this.group = group;
	}
	
	@Override
	public RdmaStoragePassiveEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
		return new RdmaStoragePassiveEndpoint(group, id, serverSide);
	}
}