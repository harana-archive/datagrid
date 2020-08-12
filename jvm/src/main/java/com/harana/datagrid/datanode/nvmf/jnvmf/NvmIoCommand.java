package com.harana.datagrid.datanode.nvmf.jnvmf;

public abstract class NvmIoCommand<C extends NvmIoCommandCapsule> extends NvmCommand<C> {

  NvmIoCommand(IoQueuePair queuePair, C commandCapsule) {
    super(queuePair, commandCapsule);
  }
}
