package com.harana.datagrid.datanode.nvmf.jnvmf;

public class AdminResponseCapsule extends ResponseCapsule<AdminCompletionQueueEntry> {

  AdminResponseCapsule() {
    super(new AdminCompletionQueueEntry());
  }
}
