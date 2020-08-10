package com.harana.datagrid.storage.nvmf.jvnmf;

public class AdminAsynchronousEventRequestCommandCapsule extends
    AdminCommandCapsule<AdminAsynchronousEventRequestCommandSqe> {

  private static final SubmissionQueueEntryFactory<AdminAsynchronousEventRequestCommandSqe>
      sqeFactory = buffer -> new AdminAsynchronousEventRequestCommandSqe(buffer);

  AdminAsynchronousEventRequestCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
