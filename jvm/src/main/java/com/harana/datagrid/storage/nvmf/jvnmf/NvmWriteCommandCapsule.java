package com.harana.datagrid.storage.nvmf.jvnmf;

public class NvmWriteCommandCapsule extends NvmIoCommandCapsule {

  private static final SubmissionQueueEntryFactory<NvmIoCommandSqe> sqeFactory =
      buffer -> new NvmWriteCommandSqe(buffer);

  NvmWriteCommandCapsule(KeyedNativeBuffer buffer, int additionalSgls, int inCapsuleDataOffset,
      int inCapsuleDataSize) {
    super(buffer, sqeFactory, additionalSgls, inCapsuleDataOffset, inCapsuleDataSize);

  }

  @Override
  public NvmWriteCommandSqe getSubmissionQueueEntry() {
    return (NvmWriteCommandSqe) super.getSubmissionQueueEntry();
  }


  void setIncapsuleData(NativeBuffer incapsuleData) {
    SglDataBlockDescriptor sglDataBlockDescriptor = getSubmissionQueueEntry()
        .getSglDataBlockDescriptor();
    sglDataBlockDescriptor.setOffset(getIncapsuleDataOffset() + incapsuleData.position());
    sglDataBlockDescriptor.setLength(incapsuleData.remaining());
  }
}
