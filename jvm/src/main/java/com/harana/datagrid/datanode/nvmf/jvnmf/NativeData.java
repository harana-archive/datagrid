package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.nio.ByteOrder;

abstract class NativeData<B extends NativeBuffer> {

  private final B buffer;

  private final void setOrder() {
    /* According to NVMf Spec 1.0 - 1.3 conventions */
    this.buffer.order(ByteOrder.LITTLE_ENDIAN);
  }

  NativeData(B buffer, int size) {
    if (buffer.remaining() < size) {
      throw new IllegalArgumentException("DatagridBuffer size to small");
    }
    buffer.limit(buffer.position() + size);
    this.buffer = (B) buffer.slice();
    setOrder();
  }

  abstract void initialize();

  void reset() {
    getBuffer().clear();
    while (getBuffer().remaining() > Long.BYTES) {
      getBuffer().putLong(0);
    }
    while (getBuffer().remaining() > 0) {
      getBuffer().put((byte) 0);
    }
    initialize();
  }

  /*
   * We do not check if the buffer is null here since
   * we do not want to take the performance hit
   */
  B getBuffer() {
    return buffer;
  }
}
