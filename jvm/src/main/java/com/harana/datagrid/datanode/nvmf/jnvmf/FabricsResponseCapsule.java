package com.harana.datagrid.datanode.nvmf.jnvmf;

public class FabricsResponseCapsule extends ResponseCapsule<FabricsCompletionQueueEntry> {

  public FabricsResponseCapsule() {
    super(new FabricsCompletionQueueEntry());
  }
}
