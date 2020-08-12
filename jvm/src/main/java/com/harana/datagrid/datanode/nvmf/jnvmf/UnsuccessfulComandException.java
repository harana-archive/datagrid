package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public class UnsuccessfulComandException extends IOException {

  private static String format(CompletionQueueEntry cqe) {
    return "{StatusCodeType: " + cqe.getStatusCodeType().toInt() + " - "
        + cqe.getStatusCodeType().getDescription()
        + ", SatusCode: " + cqe.getStatusCode().toInt() + " - "
        + cqe.getStatusCode().getDescription()
        + ", CID: " + cqe.getCommandIdentifier()
        + ", Do_not_retry: " + cqe.getDoNotRetry()
        + ", More: " + cqe.getMore()
        + ", SQHD: " + cqe.getSubmissionQueueHeadPointer() + "}";
  }

  public UnsuccessfulComandException(CompletionQueueEntry cqe) {
    super("Command was not successful. " + format(cqe));
  }
}
