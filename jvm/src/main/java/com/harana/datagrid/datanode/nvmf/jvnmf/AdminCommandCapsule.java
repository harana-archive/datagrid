package com.harana.datagrid.datanode.nvmf.jvnmf;

public abstract class AdminCommandCapsule<SqeT extends AdminSubmissionQeueueEntry> extends
    CommandCapsule<SqeT> {

  AdminCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    super(buffer, sqeFactory);
  }
}