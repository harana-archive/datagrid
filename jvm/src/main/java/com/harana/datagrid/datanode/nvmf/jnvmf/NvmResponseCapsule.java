package com.harana.datagrid.datanode.nvmf.jnvmf;

public class NvmResponseCapsule extends ResponseCapsule<NvmCompletionQueueEntry> {

  public NvmResponseCapsule() {
    super(new NvmCompletionQueueEntry());
  }
}
