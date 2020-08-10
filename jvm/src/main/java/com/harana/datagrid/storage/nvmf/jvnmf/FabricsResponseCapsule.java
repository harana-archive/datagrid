package com.harana.datagrid.storage.nvmf.jvnmf;

public class FabricsResponseCapsule extends ResponseCapsule<FabricsCompletionQueueEntry> {

  public FabricsResponseCapsule() {
    super(new FabricsCompletionQueueEntry());
  }
}
