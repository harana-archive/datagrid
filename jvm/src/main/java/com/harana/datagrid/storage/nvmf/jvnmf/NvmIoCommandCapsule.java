package com.harana.datagrid.storage.nvmf.jvnmf;

public class NvmIoCommandCapsule extends NvmCommandCapsule<NvmIoCommandSqe> {

  NvmIoCommandCapsule(KeyedNativeBuffer buffer,
      SubmissionQueueEntryFactory<NvmIoCommandSqe> sqeFactory,
      int additionalSgls, int incapsuleDataOffset, int incapsuleDataSize) {
    super(buffer, sqeFactory, additionalSgls, incapsuleDataOffset, incapsuleDataSize);
  }

  public void setSglDescriptor(KeyedNativeBuffer data) {
    //TODO: set incapsule data to zero size
    KeyedSglDataBlockDescriptor keyedSglDataBlockDescriptor =
        getSubmissionQueueEntry().getKeyedSglDataBlockDescriptor();
    keyedSglDataBlockDescriptor.set(data);
  }
}
