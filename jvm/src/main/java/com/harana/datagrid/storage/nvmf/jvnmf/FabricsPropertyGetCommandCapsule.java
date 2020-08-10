package com.harana.datagrid.storage.nvmf.jvnmf;

public class FabricsPropertyGetCommandCapsule extends CommandCapsule<FabricsPropertyGetCommandSqe> {

  private static final SubmissionQueueEntryFactory<FabricsPropertyGetCommandSqe> sqeFactory =
      buffer -> new FabricsPropertyGetCommandSqe(buffer);

  FabricsPropertyGetCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
