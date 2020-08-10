package com.harana.datagrid.storage.nvmf.jvnmf;

import com.harana.datagrid.rdma.verbs.IbvSendWR;
import com.harana.datagrid.rdma.verbs.IbvSge;
import com.harana.datagrid.rdma.verbs.SVCPostSend;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;

public abstract class Command<C extends CommandCapsule, R extends ResponseCapsule> extends
    Operation {

  private final QueuePair queuePair;
  private final C commandCapsule;
  private final IbvSendWR wr;
  private SVCPostSend postSend;

  Command(QueuePair queuePair, C commandCapsule) {
    this.queuePair = queuePair;
    this.commandCapsule = commandCapsule;
    KeyedNativeBuffer buffer = commandCapsule.getBuffer();
    IbvSge sge = new IbvSge();
    sge.setAddr(buffer.getAddress());
    sge.setLength(buffer.remaining());
    sge.setLkey(buffer.getLocalKey());

    LinkedList<IbvSge> sgList = new LinkedList<>();
    sgList.add(sge);
    this.wr = new IbvSendWR();
    wr.setSg_list(sgList);
    wr.setNum_sge(1);
    wr.setOpcode(IbvSendWR.IbvWrOcode.IBV_WR_SEND.ordinal());
    wr.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
  }

  public void setSendInline(boolean sendInline) {
    if (sendInline) {
      LinkedList<IbvSge> sgList = wr.getSg_list();
      long size = 0;
      for (IbvSge sge : sgList) {
        size += sge.getLength();
      }
      if (getQueuePair().getInlineDataSize() >= size) {
        wr.setSend_flags(wr.getSend_flags() | IbvSendWR.IBV_SEND_INLINE);
      } else {
        throw new IllegalArgumentException("Insufficient inline data size");
      }
    } else {
      wr.setSend_flags(wr.getSend_flags() & ~IbvSendWR.IBV_SEND_INLINE);
    }
  }

  private SVCPostSend getPostSend() throws IOException {
    if (postSend == null) {
      postSend = getQueuePair().newPostSend(Arrays.asList(wr));
    }
    return postSend;
  }

  public Response<R> execute(Response<R> response) throws IOException {
    getQueuePair().post(this, getPostSend(), response);
    return response;
  }

  public ResponseFuture<R> execute(ResponseFuture<R> responseFuture) throws IOException {
    getQueuePair().post(this, getPostSend(), responseFuture.getOperation());
    return responseFuture;
  }

  public abstract Response<R> newResponse();

  public ResponseFuture<R> newResponseFuture() {
    return new ResponseFuture<>(getQueuePair(), newResponse());
  }

  public CommandFuture<C, R> newCommandFuture() {
    return new CommandFuture<>(getQueuePair(), this);
  }

  void setCommandId(short commandId) {
    commandCapsule.getSubmissionQueueEntry().setCommandIdentifier(commandId);
  }

  public QueuePair getQueuePair() {
    return queuePair;
  }

  public C getCommandCapsule() {
    return commandCapsule;
  }
}
