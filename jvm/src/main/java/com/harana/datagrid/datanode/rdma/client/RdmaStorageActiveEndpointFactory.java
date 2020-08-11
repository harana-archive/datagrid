package com.harana.datagrid.datanode.rdma.client;

import com.harana.datagrid.rdma.RdmaEndpointFactory;
import com.harana.datagrid.rdma.verbs.RdmaCmId;

import java.io.IOException;

public class RdmaStorageActiveEndpointFactory implements RdmaEndpointFactory<RdmaStorageActiveEndpoint> {
	private RdmaStorageActiveGroup group;
	
	public RdmaStorageActiveEndpointFactory(RdmaStorageActiveGroup group) {
		this.group = group;
	}
	
	@Override
	public RdmaStorageActiveEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
		return new RdmaStorageActiveEndpoint(group, id, serverSide);
	}
}
