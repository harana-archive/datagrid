package com.harana.datagrid.datanode.nvmf.jnvmf;

import com.harana.datagrid.rdma.verbs.SVCPostRecv;
import com.harana.datagrid.rdma.verbs.StatefulVerbCall;

import java.io.IOException;

class RdmaRecv implements StatefulVerbCall<RdmaRecv> {

  private final SVCPostRecv postRecv;
  private final KeyedNativeBuffer buffer;

  RdmaRecv(SVCPostRecv postRecv, KeyedNativeBuffer buffer) {
    this.postRecv = postRecv;
    this.buffer = buffer;
  }

  @Override
  public RdmaRecv execute() throws IOException {
    postRecv.execute();
    return this;
  }

  @Override
  public boolean isValid() {
    return postRecv.isValid();
  }

  @Override
  public RdmaRecv free() {
    postRecv.free();
    return this;
  }

  public KeyedNativeBuffer getBuffer() {
    return buffer;
  }
}
