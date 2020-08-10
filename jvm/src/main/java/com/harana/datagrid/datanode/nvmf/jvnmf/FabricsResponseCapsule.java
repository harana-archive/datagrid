package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsResponseCapsule extends ResponseCapsule<FabricsCompletionQueueEntry> {

  public FabricsResponseCapsule() {
    super(new FabricsCompletionQueueEntry());
  }
}
