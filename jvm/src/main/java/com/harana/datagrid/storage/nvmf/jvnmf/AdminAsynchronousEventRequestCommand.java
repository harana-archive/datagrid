package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;

public class AdminAsynchronousEventRequestCommand extends
    Command<AdminAsynchronousEventRequestCommandCapsule,
        AdminAsynchronousEventRequestResponseCapsule> {

  public AdminAsynchronousEventRequestCommand(AdminQueuePair queuePair) throws IOException {
    super(queuePair,
        new AdminAsynchronousEventRequestCommandCapsule(queuePair.allocateCommandCapsule()));
  }

  @Override
  public Response<AdminAsynchronousEventRequestResponseCapsule> newResponse() {
    return new Response<>(new AdminAsynchronousEventRequestResponseCapsule());
  }
}
