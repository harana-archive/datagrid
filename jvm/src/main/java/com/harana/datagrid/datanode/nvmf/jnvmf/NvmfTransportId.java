package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.net.InetSocketAddress;
import java.util.Objects;

public class NvmfTransportId {

  private final InetSocketAddress address;
  private final NvmeQualifiedName subsystemNqn;

  public NvmfTransportId(InetSocketAddress address, NvmeQualifiedName subsystemNqn) {
    this.address = address;
    this.subsystemNqn = subsystemNqn;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  public NvmeQualifiedName getSubsystemNqn() {
    return subsystemNqn;
  }

  @Override
  public String toString() {
    return "Transport address = " + address + ", subsystem NQN = " + subsystemNqn;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    NvmfTransportId that = (NvmfTransportId) obj;
    return Objects.equals(address, that.address) && Objects.equals(subsystemNqn, that.subsystemNqn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(address, subsystemNqn);
  }
}
