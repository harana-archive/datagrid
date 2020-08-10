package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class AdminQueuePair extends QueuePair {

  /* NVMf Spec 1.0 - 5.3
   *  32 entries is the minimum admin submission queue size */
  static final short MINIMUM_SUBMISSION_QUEUE_SIZE = 32;

  AdminQueuePair(Controller controller) throws IOException {
    /* NVMf Spec 1.0 - 7.3.2 Admin and Fabrics command do not carry any incapsule data */
    super(controller, QueueId.ADMIN, MINIMUM_SUBMISSION_QUEUE_SIZE);
  }

  @Override
  FabricsConnectResponseCqe connect(long timeout, TimeUnit timeoutUnit) throws IOException {
    FabricsConnectResponseCqe cqe = super.connect(timeout, timeoutUnit);
    getController().setControllerId(cqe.success().getControllerId());
    return cqe;
  }
}
