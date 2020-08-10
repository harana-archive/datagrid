package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminResponseCapsule extends ResponseCapsule<AdminCompletionQueueEntry> {

  AdminResponseCapsule() {
    super(new AdminCompletionQueueEntry());
  }
}
