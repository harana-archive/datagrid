package com.harana.datagrid.datanode.nvmf.jvnmf;

import com.harana.datagrid.rdma.verbs.IbvMr;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RdmaByteBuffer extends NativeByteBuffer implements KeyedNativeBuffer {

  private IbvMr mr;

  RdmaByteBuffer(ByteBuffer buffer, IbvMr mr) {
    super(buffer);
    this.mr = mr;
  }

  private void checkValid() {
    if (!isValid()) {
      throw new IllegalStateException("Invalid state - deregistered");
    }
  }

  @Override
  protected RdmaByteBuffer construct(ByteBuffer buffer) {
    return new RdmaByteBuffer(buffer, mr);
  }

  @Override
  public int getRemoteKey() {
    checkValid();
    return mr.getRkey();
  }

  @Override
  public int getLocalKey() {
    checkValid();
    return mr.getLkey();
  }

  @Override
  public void free() throws IOException {
    checkValid();
    mr.deregMr().execute().free();
  }

  @Override
  public boolean isValid() {
    return mr.isOpen();
  }
}
