package com.harana.datagrid.storage.nvmf.jvnmf;

public class Pow2Size {

  final int pow2Size;

  Pow2Size(int pow2Size) {
    if (pow2Size > Integer.SIZE) {
      throw new IllegalArgumentException("Size to large");
    }
    this.pow2Size = pow2Size;
  }

  int value() {
    return pow2Size;
  }

  public int toInt() {
    return 1 << pow2Size;
  }

  @Override
  public String toString() {
    return Integer.toString(toInt());
  }
}
