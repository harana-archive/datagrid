package com.harana.datagrid.storage.nvmf.jvnmf;

public class NvmeVersion extends NativeData<NativeBuffer> {

  static final int SIZE = 4;

  private static final int TERITARY_VERSION_NUMBER_OFFSET = 0;
  private static final int MINOR_VERSION_NUMBER_OFFSET = 1;
  private static final int MAJOR_VERSION_NUMBER_OFFSET = 2;

  NvmeVersion(NativeBuffer buffer) {
    super(buffer, SIZE);
  }

  public short getMajor() {
    return getBuffer().getShort(MAJOR_VERSION_NUMBER_OFFSET);
  }

  public byte getMinor() {
    return getBuffer().get(MINOR_VERSION_NUMBER_OFFSET);
  }

  public byte getTertiary() {
    return getBuffer().get(TERITARY_VERSION_NUMBER_OFFSET);
  }

  @Override
  void initialize() {

  }

  @Override
  public String toString() {
    return getMajor() + "." + getMinor() + "." + getTertiary();
  }
}
