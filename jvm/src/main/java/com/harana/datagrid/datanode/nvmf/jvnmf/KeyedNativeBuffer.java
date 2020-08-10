package com.harana.datagrid.datanode.nvmf.jvnmf;

public interface KeyedNativeBuffer extends NativeBuffer {

  int getRemoteKey();

  int getLocalKey();
}
