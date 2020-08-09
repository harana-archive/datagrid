package com.harana.datagrid.storage.rdma;

import java.io.IOException;

import com.harana.datagrid.rdma.*;
import com.harana.datagrid.rdma.verbs.*;

public class RdmaStorageEndpointFactory implements RdmaEndpointFactory<RdmaStorageServerEndpoint> {
	private RdmaStorageServer closer;
	private RdmaActiveEndpointGroup<RdmaStorageServerEndpoint> group;
	
	public RdmaStorageEndpointFactory(RdmaActiveEndpointGroup<RdmaStorageServerEndpoint> group, RdmaStorageServer closer){
		this.group = group;
		this.closer = closer;
	}
	
	@Override
	public RdmaStorageServerEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
		return new RdmaStorageServerEndpoint(group, id, closer, serverSide);
	}
}
