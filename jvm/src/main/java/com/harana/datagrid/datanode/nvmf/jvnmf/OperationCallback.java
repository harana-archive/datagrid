package com.harana.datagrid.datanode.nvmf.jvnmf;

public interface OperationCallback {

  void onStart();

  void onComplete();

  void onFailure(RdmaException exception);
}
