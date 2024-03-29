package com.harana.datagrid.datanode.nvmf.jnvmf;

public class AdminKeepAliveCommandCapsule extends AdminCommandCapsule<AdminKeepAliveCommandSqe> {

  private static final SubmissionQueueEntryFactory<AdminKeepAliveCommandSqe> sqeFactory =
      buffer -> new AdminKeepAliveCommandSqe(buffer);

  AdminKeepAliveCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
