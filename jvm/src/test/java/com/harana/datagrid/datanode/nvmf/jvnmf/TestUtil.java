package com.harana.datagrid.datanode.nvmf.jvnmf;

public class TestUtil {

  final static String getLocalAddress() {
    String address = System.getProperty("localAddress");
    if (address == null) {
      throw new IllegalArgumentException("set -DlocalAddress");
    }
    return address;
  }

  final static String getDestinationAddress() {
    String address = System.getProperty("destinationAddress");
    if (address == null) {
      throw new IllegalArgumentException("set -DdestinationAddress");
    }
    return address;
  }

  final static int getPort() {
    String port = System.getProperty("port");
    if (port != null) {
      return Integer.parseInt(port);
    } else {
      return 50025;
    }
  }

  final static NvmeQualifiedName getSubsystemNQN() {
    String nqn = System.getProperty("nqn");
    if (nqn != null) {
      return new NvmeQualifiedName(nqn);
    } else {
      return new NvmeQualifiedName("nqn.2016-06.io.spdk:cnode1");
    }
  }
}
