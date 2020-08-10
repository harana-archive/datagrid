package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;

public class NvmReadCommand extends NvmIoCommand<NvmReadCommandCapsule> {

  public NvmReadCommand(IoQueuePair queuePair) throws IOException {
    /* read does not have any incapsule data */
    super(queuePair, new NvmReadCommandCapsule(queuePair.allocateCommandCapsule(),
        queuePair.getMaximumAdditionalSgls()));
  }
}
