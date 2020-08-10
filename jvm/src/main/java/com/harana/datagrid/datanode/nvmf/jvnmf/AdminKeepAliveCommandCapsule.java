package com.harana.datagrid.datanode.nvmf.jvnmf;

public class AdminKeepAliveCommandCapsule extends AdminCommandCapsule<AdminKeepAliveCommandSqe> {

  private static final SubmissionQueueEntryFactory<AdminKeepAliveCommandSqe> sqeFactory =
      buffer -> new AdminKeepAliveCommandSqe(buffer);

  AdminKeepAliveCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
