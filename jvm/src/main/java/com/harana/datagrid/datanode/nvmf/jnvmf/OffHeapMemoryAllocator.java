package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.nio.ByteBuffer;

public class OffHeapMemoryAllocator implements MemoryAllocator {

  @Override
  public NativeBuffer allocate(int size) {
    return new NativeByteBuffer(ByteBuffer.allocateDirect(size));
  }
}
