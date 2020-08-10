package com.harana.datagrid.datanode.nvmf.jvnmf;

public class Operation {

  private OperationCallback callback;

  final OperationCallback getCallback() {
    return callback;
  }

  public final void setCallback(OperationCallback callback) {
    this.callback = callback;
  }
}
