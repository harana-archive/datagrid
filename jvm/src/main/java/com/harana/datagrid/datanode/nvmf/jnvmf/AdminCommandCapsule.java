package com.harana.datagrid.datanode.nvmf.jnvmf;

public abstract class AdminCommandCapsule<SqeT extends AdminSubmissionQeueueEntry> extends
    CommandCapsule<SqeT> {

  AdminCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    super(buffer, sqeFactory);
  }
}
