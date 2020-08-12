package com.harana.datagrid.datanode.nvmf.jnvmf;

public abstract class AdminCommand<C extends AdminCommandCapsule>
    extends Command<C, AdminResponseCapsule> {

  AdminCommand(AdminQueuePair queuePair, C command) {
    super(queuePair, command);
  }

  @Override
  public Response<AdminResponseCapsule> newResponse() {
    return new Response<>(new AdminResponseCapsule());
  }
}
