package com.harana.datagrid.storage.nvmf.jvnmf;

public class RdmaCmAcceptPrivateData extends NativeData<NativeBuffer> {

  public static final int SIZE = 32;
  private static final int RECORD_FORMAT_OFFSET = 0;
  private static final int RDMA_QP_RECEIVE_QUEUE_SIZE_OFFSET = 2;

  //TODO implement in DiSNI
  RdmaCmAcceptPrivateData(NativeBuffer buffer) {
    super(buffer, SIZE);
  }

  short getRecordFormat() {
    return getBuffer().getShort(RECORD_FORMAT_OFFSET);
  }

  short getRdmaQpReceiveQueueSize() {
    return getBuffer().getShort(RDMA_QP_RECEIVE_QUEUE_SIZE_OFFSET);
  }

  @Override
  void initialize() {
  }
}
