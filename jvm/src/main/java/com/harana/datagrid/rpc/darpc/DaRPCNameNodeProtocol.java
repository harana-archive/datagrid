package com.harana.datagrid.rpc.darpc;

import com.harana.datagrid.darpc.DaRPCProtocol;

public class DaRPCNameNodeProtocol implements DaRPCProtocol<DaRPCNameNodeRequest, DaRPCNameNodeResponse> {

	@Override
	public DaRPCNameNodeRequest createRequest() {
		return new DaRPCNameNodeRequest();
	}

	@Override
	public DaRPCNameNodeResponse createResponse() {
		return new DaRPCNameNodeResponse();
	}
}
