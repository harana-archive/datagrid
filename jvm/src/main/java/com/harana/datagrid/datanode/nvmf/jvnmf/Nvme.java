package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Nvme {
  private static final long CONTROLLER_CONNECT_TIMEOUT = 1;
  private static final TimeUnit CONTROLLER_CONNTECT_TIMEOUT_UNIT = TimeUnit.SECONDS;

  private final NvmeQualifiedName hostNvmeQualifiedName;

  public Nvme(NvmeQualifiedName hostNvmeQualifiedName) {
    if (hostNvmeQualifiedName == null) {
      throw new IllegalArgumentException("Host NQN null");
    }
    this.hostNvmeQualifiedName = hostNvmeQualifiedName;
  }

  public Nvme() {
    this(new NvmeQualifiedName(UUID.randomUUID()));
  }

  public Controller connect(NvmfTransportId transportId) throws IOException {
    return connect(transportId, CONTROLLER_CONNECT_TIMEOUT, CONTROLLER_CONNTECT_TIMEOUT_UNIT,
        true);
  }

  public Controller connect(NvmfTransportId transportId, long connectTimeout,
      TimeUnit connectTimeoutUnit, boolean dynamicId) throws IOException {
    return new Controller(hostNvmeQualifiedName, transportId, connectTimeout, connectTimeoutUnit,
        dynamicId);
  }

  public NvmeQualifiedName getHostNvmeQualifiedName() {
    return hostNvmeQualifiedName;
  }
}
