package com.harana.datagrid.datanode.nvmf.jnvmf;

public interface OperationCallback {

  void onStart();

  void onComplete();

  void onFailure(RdmaException exception);
}
