package com.harana.datagrid.datanode.nvmf.jnvmf;

public class CommandFuture<C extends CommandCapsule, R extends ResponseCapsule>
    extends OperationFuture<Command<C, R>, C> {

  CommandFuture(QueuePair queuePair, Command<C, R> command) {
    super(queuePair, command);
  }

  @Override
  C getT() {
    return getOperation().getCommandCapsule();
  }
}
