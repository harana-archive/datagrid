package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsCompletionQueueEntry extends CompletionQueueEntry {

  private StatusCode.Value statusCodeValue;

  @Override
  public final StatusCode.Value getStatusCode() {
    return statusCodeValue;
  }

  @Override
  void update(NativeBuffer buffer) {
    super.update(buffer);
    statusCodeValue = getFabricsStatusCode();
  }
}
