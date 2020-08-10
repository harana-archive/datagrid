package com.harana.datagrid.datanode.nvmf.jvnmf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class NativeNvmeQualifiedNameTest {

  private NativeBuffer buffer;


  @BeforeEach
  void init() {
    buffer = new NativeByteBuffer(ByteBuffer.allocateDirect(NativeNvmeQualifiedName.SIZE));
  }

  @Test
  void idempotent() {
    String nqn = "nqn.2014-08.com.example:nvme:nvm-subsystem-sn-d78432";
    NativeNvmeQualifiedName nativeNQN = new NativeNvmeQualifiedName(buffer);
    nativeNQN.set(new NvmeQualifiedName(nqn));
    String decodeNqn = nativeNQN.get().toString();
    assertEquals(nqn.length(), decodeNqn.length());
    for (int i = 0; i < Math.min(nqn.length(), decodeNqn.length()); i++) {
      if (nqn.charAt(i) != decodeNqn.charAt(i)) {
        assertTrue(false, "Expected: " + Integer.toHexString(nqn.charAt(i)) + ", but was: " +
            Integer.toHexString(decodeNqn.charAt(i)));
      }
    }
  }

  @Test
  void stringToLarge() {
    StringBuilder str = new StringBuilder("nqn.2014-08.com.example:nvme:nvm-subsystem-sn-d78432");
    for (int i = str.length(); i < NativeNvmeQualifiedName.SIZE + 2; i++) {
      str.append('a');
    }
    NvmeQualifiedName nqn = new NvmeQualifiedName(str.toString());
    NativeNvmeQualifiedName nativeNQN = new NativeNvmeQualifiedName(buffer);
    assertThrows(IllegalArgumentException.class, () -> nativeNQN.set(nqn));
  }
}