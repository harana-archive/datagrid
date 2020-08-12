package com.harana.datagrid.datanode.nvmf.jnvmf;

public interface MemoryAllocator {

  NativeBuffer allocate(int size);
}
