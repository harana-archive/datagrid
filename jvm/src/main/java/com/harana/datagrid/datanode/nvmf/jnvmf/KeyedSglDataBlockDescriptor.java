package com.harana.datagrid.datanode.nvmf.jnvmf;

public class KeyedSglDataBlockDescriptor extends ScatterGatherListDescriptor {

  private static final int ADDRESS_OFFSET = 0;
  private static final int LENGTH_OFFSET = 8;
  private static final int KEY_OFFSET = 11;

  static class SubType extends ScatterGatherListDescriptor.SubType {

    class Value extends ScatterGatherListDescriptor.SubType.Value {

      Value(int value) {
        super(value);
      }
    }

    // CHECKSTYLE_OFF: MemberNameCheck

    /* address field specifies starting 64bit address of data block */
    public final Value ADDRESS = new Value(0x0);
    /* Controller should remotely invalidate RKEY if not supported ignored */
    public final Value INVALIDATE_KEY = new Value(0xF);

    // CHECKSTYLE_ON: MemberNameCheck

    private SubType() {
    }

    private static final SubType instance = new SubType();

    public static SubType getInstance() {
      return instance;
    }
  }

  KeyedSglDataBlockDescriptor(NativeBuffer buffer) {
    super(buffer);
  }

  private void setSubType(SubType.Value subType) {
    setIdentifier(Type.getInstance().KEYED_SGL_DATABLOCK, subType);
  }

  public void setAddress(long address) {
    setSubType(SubType.getInstance().ADDRESS);
    getBuffer().putLong(ADDRESS_OFFSET, address);
  }

  void invalidateRemoteKey() {
    setSubType(SubType.getInstance().INVALIDATE_KEY);
  }

  public void setLength(int length) {
    if ((length & 0xFFFFFF) != length) {
      throw new IllegalArgumentException("Invalid length. Max 3 bytes.");
    }
    int currentOffset = LENGTH_OFFSET;
    getBuffer().putShort(currentOffset, (short) length);
    length = length >> Short.SIZE;
    getBuffer().put(currentOffset + Short.BYTES, (byte) length);
  }

  public void setKey(int key) {
    getBuffer().putInt(KEY_OFFSET, key);
  }

  public void set(KeyedNativeBuffer buffer) {
    setAddress(buffer.getAddress() + buffer.position());
    setLength(buffer.remaining());
    setKey(buffer.getRemoteKey());
  }

  @Override
  void initialize() {

  }
}
