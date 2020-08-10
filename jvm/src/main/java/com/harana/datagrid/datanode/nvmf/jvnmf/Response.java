package com.harana.datagrid.datanode.nvmf.jvnmf;

public class Response<R extends ResponseCapsule> extends Operation {

  private final R responseCapsule;

  public Response(R responseCapsule) {
    this.responseCapsule = responseCapsule;
  }

  void update(NativeBuffer buffer) {
    responseCapsule.getCompletionQueueEntry().update(buffer);
  }

  public R getResponseCapsule() {
    return responseCapsule;
  }
}
