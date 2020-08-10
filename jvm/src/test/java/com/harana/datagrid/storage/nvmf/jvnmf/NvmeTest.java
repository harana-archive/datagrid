package com.harana.datagrid.storage.nvmf.jvnmf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class NvmeTest {

  @Test
  void hostNQN() {
    Nvme nvme = new Nvme();
    NvmeQualifiedName uuidNqn = nvme.getHostNvmeQualifiedName();
    assertTrue(uuidNqn.toString().startsWith("nqn.2014-08.org.nvmexpress:uuid:"));

    String nqn = "nqn.2014-08.com.example:nvme.host.sys.xyz";
    nvme = new Nvme(new NvmeQualifiedName(nqn));
    assertEquals(nqn, nvme.getHostNvmeQualifiedName().toString());
  }

  @Tag("rdma")
  @Test
  void connectTest() throws IOException {
    /* assume target is started */
    Nvme nvme = new Nvme();
    InetSocketAddress socketAddress = new InetSocketAddress(TestUtil.getDestinationAddress(),
        TestUtil.getPort());
    NvmfTransportId transportId = new NvmfTransportId(socketAddress, TestUtil.getSubsystemNQN());
    assertTrue(nvme.connect(transportId) != null);
    assertTrue(nvme.connect(transportId, 5000, TimeUnit.MILLISECONDS, true) != null);
  }
}