package com.harana.datagrid.storage.nvmf.jvnmf;

import com.harana.datagrid.rdma.RdmaEndpointFactory;
import com.harana.datagrid.rdma.verbs.RdmaCmId;

import java.io.IOException;

class NvmfRdmaEndpointFactory implements RdmaEndpointFactory<NvmfRdmaEndpoint> {

  private final NvmfRdmaEndpointGroup group;

  NvmfRdmaEndpointFactory(NvmfRdmaEndpointGroup group) {
    this.group = group;
  }

  public NvmfRdmaEndpoint createEndpoint(RdmaCmId id, boolean serverSide) throws IOException {
    NvmfRdmaEndpoint endpoint = new NvmfRdmaEndpoint(group, id);
    return endpoint;
  }
}
