package com.harana.datagrid.datanode.nvmf.jvnmf;

public class FabricsPropertyGetCommandCapsule extends CommandCapsule<FabricsPropertyGetCommandSqe> {

  private static final SubmissionQueueEntryFactory<FabricsPropertyGetCommandSqe> sqeFactory =
      buffer -> new FabricsPropertyGetCommandSqe(buffer);

  FabricsPropertyGetCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
