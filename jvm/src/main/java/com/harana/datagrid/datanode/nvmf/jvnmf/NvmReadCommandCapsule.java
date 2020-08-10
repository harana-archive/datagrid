package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NvmReadCommandCapsule extends NvmIoCommandCapsule {

  private static SubmissionQueueEntryFactory<NvmIoCommandSqe> sqeFactory =
      buffer -> new NvmReadCommandSqe(buffer);

  NvmReadCommandCapsule(KeyedNativeBuffer buffer, int additionalSgls) {
    super(buffer, sqeFactory, additionalSgls, 0, 0);
  }

  @Override
  public NvmReadCommandSqe getSubmissionQueueEntry() {
    return (NvmReadCommandSqe) super.getSubmissionQueueEntry();
  }
}
