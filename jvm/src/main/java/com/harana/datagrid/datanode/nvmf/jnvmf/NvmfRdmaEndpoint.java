package com.harana.datagrid.datanode.nvmf.jnvmf;

import com.harana.datagrid.rdma.RdmaEndpoint;
import com.harana.datagrid.rdma.verbs.IbvRecvWR;
import com.harana.datagrid.rdma.verbs.IbvSge;
import com.harana.datagrid.rdma.verbs.IbvWC;
import com.harana.datagrid.rdma.verbs.RdmaCmId;
import com.harana.datagrid.rdma.verbs.RdmaConnParam;
import com.harana.datagrid.rdma.verbs.SVCPollCq;
import com.harana.datagrid.rdma.verbs.SVCPostRecv;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.LinkedList;

class NvmfRdmaEndpoint extends RdmaEndpoint {

  private int cqSize;
  private int rqSize;
  private int sqSize;
  private int inlineDataSize;
  private final RdmaConnParam rdmaConnParam;

  private RdmaRecv[] rdmaReceives;

  NvmfRdmaEndpoint(NvmfRdmaEndpointGroup group, RdmaCmId idPriv) throws IOException {
    super(group, idPriv, false);
    this.rdmaConnParam = new RdmaConnParam();
  }

  @Override
  public synchronized void connect(SocketAddress dst, int timeoutMs) throws Exception {
    if (getCqSize() == 0) {
      throw new IllegalArgumentException("CQ size 0");
    } else if (getRqSize() == 0) {
      throw new IllegalArgumentException("RQ size 0");
    } else if (getSqSize() == 0) {
      throw new IllegalArgumentException("SQ size 0");
    }
    super.connect(dst, timeoutMs);
  }

  @Override
  public RdmaConnParam getConnParam() {
    return rdmaConnParam;
  }

  @Override
  protected synchronized void init() throws IOException {
    super.init();
    rdmaReceives = new RdmaRecv[getRqSize()];
    KeyedNativeBufferPool responseBufferPool = getBufferPool(ResponseCapsule.SIZE);
    for (int i = 0; i < getRqSize(); i++) {
      KeyedNativeBuffer buffer = responseBufferPool.allocate();
      IbvSge recvSge = new IbvSge();
      recvSge.setLkey(buffer.getLocalKey());
      recvSge.setLength(buffer.capacity());
      recvSge.setAddr(buffer.getAddress());

      IbvRecvWR recvWr = new IbvRecvWR();
      recvWr.setWr_id(i);
      recvWr.setSg_list(new LinkedList<>());
      recvWr.getSg_list().add(recvSge);
      recvWr.setNum_sge(1);

      SVCPostRecv postRecv = postRecv(Arrays.asList(recvWr)).execute();
      rdmaReceives[i] = new RdmaRecv(postRecv, buffer);
    }
  }

  public RdmaRecv[] getRdmaReceives() {
    return rdmaReceives.clone();
  }

  public class PollCq extends SVCPollCq {

    private final SVCPollCq pollCq;
    private final IbvWC[] wcs;

    public PollCq(int polls) throws IOException {
      this.wcs = new IbvWC[polls];
      for (int i = 0; i < wcs.length; i++) {
        wcs[i] = new IbvWC();
      }
      this.pollCq = getCqProvider().getCQ().poll(wcs, wcs.length);
    }

    @Override
    public int getPolls() {
      return pollCq.getPolls();
    }

    @Override
    public SVCPollCq execute() throws IOException {
      return pollCq.execute();
    }

    @Override
    public boolean isValid() {
      return pollCq.isValid();
    }

    @Override
    public SVCPollCq free() {
      return pollCq.free();
    }

    public IbvWC[] getWorkCompletions() {
      return wcs;
    }
  }

  public void setCqSize(int cqSize) {
    if (cqSize <= 0) {
      throw new IllegalArgumentException("CQ size <= 0");
    }
    this.cqSize = cqSize;
  }

  public int getCqSize() {
    return cqSize;
  }

  public int getInlineDataSize() {
    return inlineDataSize;
  }

  public void setInlineDataSize(int inlineDataSize) {
    if (inlineDataSize < 0) {
      throw new IllegalArgumentException("Inline data size negative");
    }
    this.inlineDataSize = inlineDataSize;
  }

  public int getRqSize() {
    return rqSize;
  }

  public void setRqSize(int rqSize) {
    if (rqSize <= 0) {
      throw new IllegalArgumentException("RQ size <= 0");
    }
    this.rqSize = rqSize;
  }

  public int getSqSize() {
    return sqSize;
  }

  public void setSqSize(int sqSize) {
    if (sqSize <= 0) {
      throw new IllegalArgumentException("SQ size <= 0");
    }
    this.sqSize = sqSize;
  }

  private NvmfRdmaEndpointGroup getEndpointGroup() {
    return (NvmfRdmaEndpointGroup) group;
  }

  public KeyedNativeBufferPool getBufferPool(int size) throws IOException {
    return getEndpointGroup().getBufferPool(this, size);
  }
}
