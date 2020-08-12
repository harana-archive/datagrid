package com.harana.datagrid.datanode.nvmf.jnvmf;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class NvmeQualifiedNameTest {
  /*
   * NVMe Spec 1.3a - 7.9
   */

  @Test
  void namingAuthority() {
    String nqn = "nqn.2014-08.com.example:nvme:nvm-subsystem-sn-d78432";
    new NvmeQualifiedName(nqn);
    nqn = "nqn.2014-08.com.example:nvme.host.sys.xyz";
    new NvmeQualifiedName(nqn);
  }

  @Test
  void uniqueIdentifier() {
    String nqn = "nqn.2014-08.org.nvmexpress:uuid:f81d4fae-7dec-11d0-a765-00a0c91e6bf6";
    new NvmeQualifiedName(nqn);
  }

  @Test
  void invalidNQN() {
    String nqn = "nqn.x";
    assertThrows(IllegalArgumentException.class, () -> new NvmeQualifiedName(nqn));
  }
}