package com.harana.datagrid.storage.nvmf.jvnmf;

public interface OperationCallback {

  void onStart();

  void onComplete();

  void onFailure(RdmaException exception);
}
