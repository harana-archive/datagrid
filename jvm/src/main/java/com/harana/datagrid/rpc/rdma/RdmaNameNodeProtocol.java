package com.harana.datagrid.rpc.rdma;

import com.harana.datagrid.darpc.DaRPCProtocol;

public class RdmaNameNodeProtocol implements DaRPCProtocol<RdmaNameNodeRequest, RdmaNameNodeResponse> {

	@Override
	public RdmaNameNodeRequest createRequest() {
		return new RdmaNameNodeRequest();
	}

	@Override
	public RdmaNameNodeResponse createResponse() {
		return new RdmaNameNodeResponse();
	}
}
