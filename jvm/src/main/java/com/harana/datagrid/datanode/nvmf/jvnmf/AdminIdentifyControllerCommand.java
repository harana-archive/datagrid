package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public class AdminIdentifyControllerCommand extends
    AdminCommand<AdminIdentifyControllerCommandCapsule> {

  public AdminIdentifyControllerCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair, new AdminIdentifyControllerCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
