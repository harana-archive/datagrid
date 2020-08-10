package com.harana.datagrid.storage.nvmf.jvnmf;

public interface KeyedNativeBuffer extends NativeBuffer {

  int getRemoteKey();

  int getLocalKey();
}
