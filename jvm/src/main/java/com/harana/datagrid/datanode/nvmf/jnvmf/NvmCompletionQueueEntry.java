package com.harana.datagrid.datanode.nvmf.jnvmf;

public class NvmCompletionQueueEntry extends CompletionQueueEntry {

  private StatusCode.Value statusCodeValue;

  @Override
  public final StatusCode.Value getStatusCode() {
    return statusCodeValue;
  }

  @Override
  void update(NativeBuffer buffer) {
    super.update(buffer);
    statusCodeValue = getNvmStatusCode();
  }
}
