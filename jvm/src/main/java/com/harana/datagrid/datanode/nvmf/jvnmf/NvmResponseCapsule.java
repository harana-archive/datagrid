package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmResponseCapsule extends ResponseCapsule<NvmCompletionQueueEntry> {

  public NvmResponseCapsule() {
    super(new NvmCompletionQueueEntry());
  }
}
