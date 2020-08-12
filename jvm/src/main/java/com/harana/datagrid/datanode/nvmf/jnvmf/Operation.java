package com.harana.datagrid.datanode.nvmf.jnvmf;

public class Operation {

  private OperationCallback callback;

  final OperationCallback getCallback() {
    return callback;
  }

  public final void setCallback(OperationCallback callback) {
    this.callback = callback;
  }
}
