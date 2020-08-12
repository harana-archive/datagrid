package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class AdminIdentifyNamespaceCommand extends
    AdminCommand<AdminIdentifyNamespaceCommandCapsule> {

  AdminIdentifyNamespaceCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair, new AdminIdentifyNamespaceCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
