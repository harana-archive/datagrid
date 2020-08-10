package com.harana.datagrid.datanode.nvmf.jvnmf;

public interface MemoryAllocator {

  NativeBuffer allocate(int size);
}
