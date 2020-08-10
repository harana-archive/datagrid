package com.harana.datagrid.storage.nvmf.jvnmf;

public abstract class NvmIoCommand<C extends NvmIoCommandCapsule> extends NvmCommand<C> {

  NvmIoCommand(IoQueuePair queuePair, C commandCapsule) {
    super(queuePair, commandCapsule);
  }
}
