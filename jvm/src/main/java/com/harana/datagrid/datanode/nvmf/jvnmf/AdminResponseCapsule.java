package com.harana.datagrid.datanode.nvmf.jvnmf;

public class AdminResponseCapsule extends ResponseCapsule<AdminCompletionQueueEntry> {

  AdminResponseCapsule() {
    super(new AdminCompletionQueueEntry());
  }
}
