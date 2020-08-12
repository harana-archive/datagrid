package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class AdminIdentifyControllerCommand extends
    AdminCommand<AdminIdentifyControllerCommandCapsule> {

  public AdminIdentifyControllerCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair, new AdminIdentifyControllerCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
