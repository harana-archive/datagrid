package com.harana.datagrid.datanode.nvmf.jnvmf;

public interface SubmissionQueueEntryFactory<SqeT extends SubmissionQueueEntry> {
  SqeT construct(NativeBuffer buffer);
}
