package com.harana.datagrid.datanode.nvmf.jvnmf;

public class ResponseFuture<R extends ResponseCapsule> extends OperationFuture<Response<R>, R> {

  ResponseFuture(QueuePair queuePair, Response<R> response) {
    super(queuePair, response);
  }

  @Override
  R getT() {
    return getOperation().getResponseCapsule();
  }
}
