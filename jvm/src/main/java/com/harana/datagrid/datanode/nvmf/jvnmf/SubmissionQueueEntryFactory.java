package com.harana.datagrid.datanode.nvmf.jvnmf;

public interface SubmissionQueueEntryFactory<SqeT extends SubmissionQueueEntry> {
  SqeT construct(NativeBuffer buffer);
}
