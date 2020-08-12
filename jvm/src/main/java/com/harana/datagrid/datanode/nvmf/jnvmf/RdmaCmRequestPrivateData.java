package com.harana.datagrid.datanode.nvmf.jnvmf;

public class RdmaCmRequestPrivateData extends NativeData<NativeBuffer> {

  static final int SIZE = 32;
  private static final int RECORD_FORMAT_OFFSET = 0;
  private static final int QUEUE_ID_OFFSET = 2;
  private static final int RDMA_QP_RECEIVE_QUEUE_SIZE_OFFSET = 4;
  private static final int RDMA_QP_SEND_QUEUE_SIZE_OFFSET = 6;

  RdmaCmRequestPrivateData(NativeBuffer buffer) {
    super(buffer, SIZE);
  }

  void setQueueId(QueueId queueId) {
    getBuffer().putShort(QUEUE_ID_OFFSET, queueId.toShort());
  }

  void setRdmaQpReceiveQueueSize(short size) {
    getBuffer().putShort(RDMA_QP_RECEIVE_QUEUE_SIZE_OFFSET, size);
  }

  void setRdmaQpSendQueueSize(short size) {
    getBuffer().putShort(RDMA_QP_SEND_QUEUE_SIZE_OFFSET, size);
  }

  @Override
  void initialize() {
    getBuffer().putShort(RECORD_FORMAT_OFFSET, (short) 0);
  }
}
