package com.harana.datagrid.storage.nvmf.jvnmf;

public class NvmFlushCommandCapsule extends NvmCommandCapsule<NvmFlushCommandSqe> {

  private static SubmissionQueueEntryFactory<NvmFlushCommandSqe> sqeFactory =
      buffer -> new NvmFlushCommandSqe(buffer);

  NvmFlushCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
