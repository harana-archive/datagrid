package com.harana.datagrid.datanode.nvmf.jnvmf;

public class NvmFlushCommandCapsule extends NvmCommandCapsule<NvmFlushCommandSqe> {

  private static SubmissionQueueEntryFactory<NvmFlushCommandSqe> sqeFactory =
      buffer -> new NvmFlushCommandSqe(buffer);

  NvmFlushCommandCapsule(KeyedNativeBuffer buffer) {
    super(buffer, sqeFactory);
  }
}
