package com.harana.datagrid.datanode.nvmf.jvnmf;

public abstract class AdminIdentifyCommandCapsule extends
    AdminCommandCapsule<AdminIdentifyCommandSqe> {

  private static final SubmissionQueueEntryFactory<AdminIdentifyCommandSqe> sqeFactory =
      buffer -> new AdminIdentifyCommandSqe(buffer);

  AdminIdentifyCommandCapsule(KeyedNativeBuffer buffer,
      AdminIdentifyCommandReturnType.Value returnType) {
    super(buffer, sqeFactory);
    getSubmissionQueueEntry().setReturnType(returnType);
  }
}
