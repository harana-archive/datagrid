package com.harana.datagrid.datanode.nvmf.jnvmf;

public class AdminGetLogPageCommandCapsule extends AdminCommandCapsule<AdminGetLogPageCommandSqe> {

  private static final SubmissionQueueEntryFactory<AdminGetLogPageCommandSqe>
          sqeFactory = buffer -> new AdminGetLogPageCommandSqe(buffer);

  AdminGetLogPageCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
