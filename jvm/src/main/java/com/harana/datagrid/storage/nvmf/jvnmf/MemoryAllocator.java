package com.harana.datagrid.storage.nvmf.jvnmf;

public interface MemoryAllocator {

  NativeBuffer allocate(int size);
}
