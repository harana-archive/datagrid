package com.harana.datagrid.storage.nvmf.jvnmf;

import java.nio.ByteBuffer;

public class OffHeapMemoryAllocator implements MemoryAllocator {

  @Override
  public NativeBuffer allocate(int size) {
    return new NativeByteBuffer(ByteBuffer.allocateDirect(size));
  }
}
