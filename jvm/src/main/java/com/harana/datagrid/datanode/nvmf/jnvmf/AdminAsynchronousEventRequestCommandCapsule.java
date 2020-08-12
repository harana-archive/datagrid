package com.harana.datagrid.datanode.nvmf.jnvmf;

public class AdminAsynchronousEventRequestCommandCapsule extends
    AdminCommandCapsule<AdminAsynchronousEventRequestCommandSqe> {

  private static final SubmissionQueueEntryFactory<AdminAsynchronousEventRequestCommandSqe>
      sqeFactory = buffer -> new AdminAsynchronousEventRequestCommandSqe(buffer);

  AdminAsynchronousEventRequestCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
