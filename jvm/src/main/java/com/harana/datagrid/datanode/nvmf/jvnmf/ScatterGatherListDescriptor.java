package com.harana.datagrid.datanode.nvmf.jvnmf;

abstract class ScatterGatherListDescriptor extends NativeData<NativeBuffer> {

  public static final int SIZE = 16;
  private static final int IDENTIFIER_OFFSET = 15;
  private static final int TYPE_BITOFFSET = 4;

  abstract static class SubType extends EEnum<SubType.Value> {

    SubType() {
      super(0xf);
    }

    class Value extends EEnum.Value {

      Value(int value) {
        super(value);
      }
    }
  }

  static class Type extends EEnum<Type.Value> {

    class Value extends EEnum.Value {

      Value(int value) {
        super(value);
      }
    }

    // CHECKSTYLE_OFF: MemberNameCheck

    public final Value SGL_DATABLOCK = new Value(0x0);

    public final Value KEYED_SGL_DATABLOCK = new Value(0x4);

    // CHECKSTYLE_ON: MemberNameCheck

    private Type() {
      super(0xf);
    }

    private static final Type instance = new Type();

    public static Type getInstance() {
      return instance;
    }
  }

  ScatterGatherListDescriptor(NativeBuffer buffer) {
    super(buffer, SIZE);
  }

  protected final void setIdentifier(Type.Value type, SubType.Value subType) {
    int identifier = subType.toInt();
    identifier = identifier | (type.toInt() << TYPE_BITOFFSET);
    getBuffer().put(IDENTIFIER_OFFSET, (byte) identifier);
  }
}
