package com.harana.datagrid.storage.nvmf.jvnmf;

public class Property {

  static class Size extends EEnum<Size.Value> {

    class Value extends EEnum.Value {

      Value(int value) {
        super(value);
      }
    }

    private Size() {
      super(1);
    }

    private static final Size instance = new Size();

    public static Size getInstance() {
      return instance;
    }

    // CHECKSTYLE_OFF: MemberNameCheck

    public final Value FOUR_BYTES = new Value(0);
    public final Value EIGHT_BYTES = new Value(1);

    // CHECKSTYLE_ON: MemberNameCheck
  }

  private final Size.Value size;
  private final int offset;

  Property(Size.Value size, int offset) {
    this.size = size;
    this.offset = offset;
  }

  public Size.Value getSize() {
    return size;
  }

  public int getOffset() {
    return offset;
  }
}
