package com.harana.datagrid.storage.nvmf.jvnmf;

public class ResponseFuture<R extends ResponseCapsule> extends OperationFuture<Response<R>, R> {

  ResponseFuture(QueuePair queuePair, Response<R> response) {
    super(queuePair, response);
  }

  @Override
  R getT() {
    return getOperation().getResponseCapsule();
  }
}
