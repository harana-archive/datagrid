package com.harana.datagrid.storage.nvmf.jvnmf;


public final class FabricsConnectCommandCapsule extends
    FabricsCommandCapsule<FabricsConnectCommandSqe> {

  private static final SubmissionQueueEntryFactory<FabricsConnectCommandSqe> sqeFactory =
      buffer -> new FabricsConnectCommandSqe(buffer);

  FabricsConnectCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }

  public void setSglDescriptor(FabricsConnectCommandData data) {
    getSubmissionQueueEntry().getKeyedSglDataBlockDescriptor().set(data.getBuffer());
  }
}
