package com.harana.datagrid.storage.rdma.client;

import java.io.IOException;

import com.harana.datagrid.rdma.verbs.*;
import com.harana.datagrid.rdma.*;

public class RdmaStorageActiveEndpointFactory implements RdmaEndpointFactory<RdmaStorageActiveEndpoint> {
	private RdmaStorageActiveGroup group;
	
	public RdmaStorageActiveEndpointFactory(RdmaStorageActiveGroup group){
		this.group = group;
	}
	
	@Override
	public RdmaStorageActiveEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
		return new RdmaStorageActiveEndpoint(group, id, serverSide);
	}
}
