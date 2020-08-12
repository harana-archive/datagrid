package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class AdminIdentifyActiveNamespaceIdsCommand extends
    AdminCommand<AdminIdentifyActiveNamespacesCommandCapsule> {

  AdminIdentifyActiveNamespaceIdsCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair,
        new AdminIdentifyActiveNamespacesCommandCapsule(queuePair.allocateCommandCapsule()));
  }
}
