package com.harana.datagrid.storage.nvmf.jvnmf;

public abstract class ResponseCapsule<CqeT extends CompletionQueueEntry> {

  /*
   * NVMf Spec 1.0 - 2.2
   *
   * RDMA transports do not contain in-capsule data in responses
   *
   */
  static final int SIZE = 16;

  private final CqeT completionQueueEntry;

  public ResponseCapsule(CqeT completionQueueEntry) {
    this.completionQueueEntry = completionQueueEntry;
  }

  public CqeT getCompletionQueueEntry() {
    return completionQueueEntry;
  }
}
