package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

final class FabricsHostIdentifier {

  /*
   * NVMe Spec 1.3a - 5.21.1.19.2
   * The Host Identifier shall be an extended 128-bit Host Identifier
   */
  public static final int SIZE = 16;

  private byte[] identifier;

  private FabricsHostIdentifier() {
  }

  public void get(NativeBuffer buffer) {
    if (buffer.remaining() < SIZE) {
      throw new IllegalArgumentException("buffer to small");
    }
    if (identifier == null) {
      try {
        Enumeration<NetworkInterface> networkInterfaceEnumeration = NetworkInterface
            .getNetworkInterfaces();
        while (networkInterfaceEnumeration.hasMoreElements() && identifier == null) {
          NetworkInterface networkInterface = networkInterfaceEnumeration.nextElement();
          identifier = networkInterface.getHardwareAddress();
        }
        if (identifier == null) {
          throw new SocketException();
        }
      } catch (SocketException exception) {
        System.err.println("WARN: Could not get MAC address for host identifier");
        exception.printStackTrace();
        identifier = new byte[]{0x12, 0x34, 0x56, 0x78};
      }
    }
    buffer.put(identifier);
  }

  private static FabricsHostIdentifier fabricsHostIdentifier;

  static FabricsHostIdentifier getInstance() {
    if (fabricsHostIdentifier == null) {
      fabricsHostIdentifier = new FabricsHostIdentifier();
    }
    return fabricsHostIdentifier;
  }
}
