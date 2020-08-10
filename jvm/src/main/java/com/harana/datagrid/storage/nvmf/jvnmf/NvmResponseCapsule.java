package com.harana.datagrid.storage.nvmf.jvnmf;

public class NvmResponseCapsule extends ResponseCapsule<NvmCompletionQueueEntry> {

  public NvmResponseCapsule() {
    super(new NvmCompletionQueueEntry());
  }
}
