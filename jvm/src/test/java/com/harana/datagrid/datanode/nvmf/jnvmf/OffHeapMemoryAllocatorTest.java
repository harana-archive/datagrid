package com.harana.datagrid.datanode.nvmf.jnvmf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;

class OffHeapMemoryAllocatorTest {

  @Test
  void overflow() {
    MemoryAllocator memoryAllocator = new OffHeapMemoryAllocator();
    final int size = 512;
    NativeBuffer buffer = memoryAllocator.allocate(size);
    byte b[] = new byte[size + 1];
    ThreadLocalRandom.current().nextBytes(b);
    assertThrows(BufferOverflowException.class, () -> buffer.put(b));
  }

  @Test
  void free() throws IOException {
    MemoryAllocator memoryAllocator = new OffHeapMemoryAllocator();
    NativeBuffer buffer = memoryAllocator.allocate(1);
    assertTrue(buffer.isValid());
    buffer.free();
    // off heap memory allocator buffers stay valid after free
    // free is no-op
    assertTrue(buffer.isValid());
  }

  @Test
  void size() {
    MemoryAllocator memoryAllocator = new OffHeapMemoryAllocator();
    ThreadLocalRandom random = ThreadLocalRandom.current();
    NativeBuffer[] buffers = new NativeBuffer[10];
    for (int i = 0; i < buffers.length; i++) {
      int size = random.nextInt(0, 1024 * 1024);
      NativeBuffer buffer = memoryAllocator.allocate(size);
      assertEquals(size, buffer.capacity());
      buffers[i] = buffer;
    }
    for (int i = 0; i < buffers.length; i++) {
      NativeBuffer a = buffers[i];
      for (int j = i + 1; j < buffers.length; j++) {
        NativeBuffer b = buffers[j];
        assertNotEquals(a, b);
        assertNotEquals(a.getAddress(), b.getAddress());
      }
    }
  }

  @Test
  void zeroSize() {
    MemoryAllocator memoryAllocator = new OffHeapMemoryAllocator();
    NativeBuffer buffer = memoryAllocator.allocate(0);
    assertEquals(0, buffer.capacity());
  }
}