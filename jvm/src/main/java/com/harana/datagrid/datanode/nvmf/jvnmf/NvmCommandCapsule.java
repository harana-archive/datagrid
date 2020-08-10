package com.harana.datagrid.datanode.nvmf.jvnmf;

public abstract class NvmCommandCapsule<SqeT extends NvmSubmissionQueueEntry> extends
    CommandCapsule<SqeT> {

  NvmCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory,
      int additionalSgls, int incapsuleDataOffset, int incapsuleDataSize) {
    super(buffer, sqeFactory, additionalSgls, incapsuleDataOffset, incapsuleDataSize);
  }

  NvmCommandCapsule(KeyedNativeBuffer buffer, SubmissionQueueEntryFactory<SqeT> sqeFactory) {
    super(buffer, sqeFactory);
  }
}
