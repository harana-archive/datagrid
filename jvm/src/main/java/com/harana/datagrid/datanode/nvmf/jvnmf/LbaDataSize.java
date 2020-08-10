package com.harana.datagrid.datanode.nvmf.jvnmf;

public class LbaDataSize extends Pow2Size {

  private static final int MINIMUM_DATA_SIZE = 9;

  LbaDataSize(int pow2Size) {
    super(pow2Size);
    if (pow2Size < MINIMUM_DATA_SIZE) {
      throw new IllegalArgumentException("Minimum LBA data size is " + MINIMUM_DATA_SIZE);
    }
  }
}
