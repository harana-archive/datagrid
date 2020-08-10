package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public class AdminIdentifyNamespaceCommand extends
    AdminCommand<AdminIdentifyNamespaceCommandCapsule> {

  AdminIdentifyNamespaceCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair, new AdminIdentifyNamespaceCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
