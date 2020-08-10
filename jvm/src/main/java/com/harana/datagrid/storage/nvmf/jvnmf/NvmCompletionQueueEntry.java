package com.harana.datagrid.storage.nvmf.jvnmf;

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
