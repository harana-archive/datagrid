package com.harana.datagrid.storage.nvmf.jvnmf;

public interface SubmissionQueueEntryFactory<SqeT extends SubmissionQueueEntry> {
  SqeT construct(NativeBuffer buffer);
}
