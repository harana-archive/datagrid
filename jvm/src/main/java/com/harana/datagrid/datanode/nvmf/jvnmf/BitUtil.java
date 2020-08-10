package com.harana.datagrid.datanode.nvmf.jvnmf;

class BitUtil {

  private static void checkBounds(int position, int limit) {
    if (position < 0) {
      throw new IllegalArgumentException("negative index " + position);
    }
    if (position >= limit) {
      throw new IllegalArgumentException("index too large " + position + ">=" + limit);
    }
  }

  static boolean getBit(int value, int position) {
    checkBounds(position, Integer.SIZE);
    return (value & (1 << position)) != 0;
  }

  static boolean getBit(long value, int position) {
    checkBounds(position, Long.SIZE);
    return (value & (1L << position)) != 0;
  }

  static int clearBit(int value, int position) {
    checkBounds(position, Integer.SIZE);
    return value & ~(1 << position);
  }

  static int setBit(int value, int position) {
    checkBounds(position, Integer.SIZE);
    return value | (1 << position);
  }

  static int setBitTo(int value, int position, boolean setTo) {
    if (setTo) {
      return setBit(value, position);
    } else {
      return clearBit(value, position);
    }
  }

  private static int getMask(int start, int end) {
    checkBounds(start, Integer.SIZE);
    checkBounds(end, Integer.SIZE);
    if (start > end) {
      throw new IllegalArgumentException("start index exceeds end");
    }
    return (int) ((1L << (end + 1 - start)) - 1L);
  }

  private static long getMaskLong(int start, int end) {
    checkBounds(start, Long.SIZE);
    checkBounds(end, Long.SIZE);
    if (start > end) {
      throw new IllegalArgumentException("start index exceeds end");
    }
    return (1L << (end + 1 - start)) - 1L;
  }

  /* [start, end] (inclusive) */
  static int getBits(int value, int start, int end) {
    int mask = getMask(start, end);
    value = value >> start;
    return value & mask;
  }

  /* [start, end] (inclusive) */
  static long getBits(long value, int start, int end) {
    long mask = getMaskLong(start, end);
    value = value >> start;
    return value & mask;
  }

  static int clearBits(int value, int start, int end) {
    int mask = getMask(start, end);
    return value & ~(mask << start);
  }

  /* [start, end] (inclusive) */
  static int setBitsTo(int value, int start, int end, int setTo) {
    value = clearBits(value, start, end);
    int mask = getMask(start, end);
    if (setTo != (setTo & mask)) {
      throw new IllegalArgumentException(
          Integer.toHexString(setTo) + " does not fit inside " + start + ":" + end);
    }
    return value | (setTo << start);
  }

  static long pop_array(long[] arr, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr[i]);
    }
    return popCount;
  }

  /** Returns the popcount or cardinality of the two sets after an intersection.
   *  Neither array is modified. */
  static long pop_intersect(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] & arr2[i]);
    }
    return popCount;
  }

  /** Returns the popcount or cardinality of the union of two sets.
   *  Neither array is modified. */
  static long pop_union(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] | arr2[i]);
    }
    return popCount;
  }

  /** Returns the popcount or cardinality of {@code A & ~B}.
   *  Neither array is modified. */
  static long pop_andnot(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] & ~arr2[i]);
    }
    return popCount;
  }

  /** Returns the popcount or cardinality of A ^ B
   * Neither array is modified. */
  static long pop_xor(long[] arr1, long[] arr2, int wordOffset, int numWords) {
    long popCount = 0;
    for (int i = wordOffset, end = wordOffset + numWords; i < end; ++i) {
      popCount += Long.bitCount(arr1[i] ^ arr2[i]);
    }
    return popCount;
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
  static int nextHighestPowerOfTwo(int v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
  }

  /** returns the next highest power of two, or the current value if it's already a power of two or zero*/
  static long nextHighestPowerOfTwo(long v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
  }

  // magic numbers for bit interleaving
  private static final long MAGIC0 = 0x5555555555555555L;
  private static final long MAGIC1 = 0x3333333333333333L;
  private static final long MAGIC2 = 0x0F0F0F0F0F0F0F0FL;
  private static final long MAGIC3 = 0x00FF00FF00FF00FFL;
  private static final long MAGIC4 = 0x0000FFFF0000FFFFL;
  private static final long MAGIC5 = 0x00000000FFFFFFFFL;
  private static final long MAGIC6 = 0xAAAAAAAAAAAAAAAAL;

  // shift values for bit interleaving
  private static final long SHIFT0 = 1;
  private static final long SHIFT1 = 2;
  private static final long SHIFT2 = 4;
  private static final long SHIFT3 = 8;
  private static final long SHIFT4 = 16;

  /**
   * Interleaves the first 32 bits of each long value
   *
   * Adapted from: http://graphics.stanford.edu/~seander/bithacks.html#InterleaveBMN
   */
  static long interleave(int even, int odd) {
    long v1 = 0x00000000FFFFFFFFL & even;
    long v2 = 0x00000000FFFFFFFFL & odd;
    v1 = (v1 | (v1 << SHIFT4)) & MAGIC4;
    v1 = (v1 | (v1 << SHIFT3)) & MAGIC3;
    v1 = (v1 | (v1 << SHIFT2)) & MAGIC2;
    v1 = (v1 | (v1 << SHIFT1)) & MAGIC1;
    v1 = (v1 | (v1 << SHIFT0)) & MAGIC0;
    v2 = (v2 | (v2 << SHIFT4)) & MAGIC4;
    v2 = (v2 | (v2 << SHIFT3)) & MAGIC3;
    v2 = (v2 | (v2 << SHIFT2)) & MAGIC2;
    v2 = (v2 | (v2 << SHIFT1)) & MAGIC1;
    v2 = (v2 | (v2 << SHIFT0)) & MAGIC0;

    return (v2<<1) | v1;
  }

  /**
   * Extract just the even-bits value as a long from the bit-interleaved value
   */
  static long deinterleave(long b) {
    b &= MAGIC0;
    b = (b ^ (b >>> SHIFT0)) & MAGIC1;
    b = (b ^ (b >>> SHIFT1)) & MAGIC2;
    b = (b ^ (b >>> SHIFT2)) & MAGIC3;
    b = (b ^ (b >>> SHIFT3)) & MAGIC4;
    b = (b ^ (b >>> SHIFT4)) & MAGIC5;
    return b;
  }

  /**
   * flip flops odd with even bits
   */
  static long flipFlop(final long b) {
    return ((b & MAGIC6) >>> 1) | ((b & MAGIC0) << 1 );
  }

  /** Same as {@link #zigZagEncode(long)} but on integers. */
  static int zigZagEncode(int i) {
    return (i >> 31) ^ (i << 1);
  }

  /**
   * <a href="https://developers.google.com/protocol-buffers/docs/encoding#types">Zig-zag</a>
   * encode the provided long. Assuming the input is a signed long whose
   * absolute value can be stored on <code>n</code> bits, the returned value will
   * be an unsigned long that can be stored on <code>n+1</code> bits.
   */
  static long zigZagEncode(long l) {
    return (l >> 63) ^ (l << 1);
  }

  /** Decode an int previously encoded with {@link #zigZagEncode(int)}. */
  static int zigZagDecode(int i) {
    return ((i >>> 1) ^ -(i & 1));
  }

  /** Decode a long previously encoded with {@link #zigZagEncode(long)}. */
  static long zigZagDecode(long l) {
    return ((l >>> 1) ^ -(l & 1));
  }
}