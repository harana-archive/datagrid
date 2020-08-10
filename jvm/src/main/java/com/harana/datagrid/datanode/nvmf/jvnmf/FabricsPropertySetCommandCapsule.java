package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsPropertySetCommandCapsule extends
    FabricsCommandCapsule<FabricsPropertySetCommandSqe> {

  private static final SubmissionQueueEntryFactory<FabricsPropertySetCommandSqe> sqeFactory =
      buffer -> new FabricsPropertySetCommandSqe(buffer);

  FabricsPropertySetCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
