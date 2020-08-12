package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class IoQueuePair extends QueuePair {

  IoQueuePair(Controller controller, QueueId queueId, short submissionQueueSize,
      int additionalSgls, int inCapsuleDataSize, int maxInlineSize) throws IOException {
    super(controller, queueId, submissionQueueSize, additionalSgls, inCapsuleDataSize,
        maxInlineSize);
  }
}
