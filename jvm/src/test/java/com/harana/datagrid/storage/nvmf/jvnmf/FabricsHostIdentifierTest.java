package com.harana.datagrid.storage.nvmf.jvnmf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class FabricsHostIdentifierTest {

  @Test
  void constant() {
    NativeBuffer buffer = new NativeByteBuffer(
        ByteBuffer.allocateDirect(FabricsHostIdentifier.SIZE));
    NativeBuffer buffer2 = new NativeByteBuffer(
        ByteBuffer.allocateDirect(FabricsHostIdentifier.SIZE));
    FabricsHostIdentifier.getInstance().get(buffer);
    buffer.clear();
    FabricsHostIdentifier.getInstance().get(buffer2);
    buffer2.clear();
    assertEquals(buffer.sliceToByteBuffer(), buffer2.sliceToByteBuffer());
  }

  @Test
  void bufferToSmall() {
    NativeBuffer buffer = new NativeByteBuffer(
        ByteBuffer.allocateDirect(FabricsHostIdentifier.SIZE - 1));
    assertThrows(IllegalArgumentException.class,
        () -> FabricsHostIdentifier.getInstance().get(buffer));
  }
}