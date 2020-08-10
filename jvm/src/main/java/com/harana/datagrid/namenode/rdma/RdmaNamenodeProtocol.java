package com.harana.datagrid.namenode.rdma;

import com.harana.datagrid.rpc.darpc.DaRPCProtocol;

public class RdmaNamenodeProtocol implements DaRPCProtocol<RdmaNamenodeRequest, RdmaNamenodeResponse> {

	@Override
	public RdmaNamenodeRequest createRequest() {
		return new RdmaNamenodeRequest();
	}

	@Override
	public RdmaNamenodeResponse createResponse() {
		return new RdmaNamenodeResponse();
	}
}
