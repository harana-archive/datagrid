package com.harana.datagrid.datanode.nvmf.jnvmf;

public interface KeyedNativeBuffer extends NativeBuffer {

  int getRemoteKey();

  int getLocalKey();
}
