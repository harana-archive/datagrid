package com.harana.datagrid.datanode.nvmf.jnvmf;

public abstract class EEnum<T extends EEnum.Value> {

  public class Value {

    private final int value;

    Value(int value) {
      this.value = value;
      try {
        if (table[value] != null) {
          throw new Exception();
        }
        table[value] = this;
      } catch (Exception exception) {
        throw new IllegalArgumentException(exception);
      }
    }

    public int toInt() {
      return value;
    }

    public byte toByte() {
      return (byte) toInt();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || !getClass().isInstance(obj)) {
        return false;
      }

      Value value1 = (Value) obj;

      return value == value1.value;
    }

    @Override
    public int hashCode() {
      return value;
    }
  }

  private final Value[] table;

  EEnum(int maxValue) {
    if (maxValue < 0) {
      throw new IllegalArgumentException("max value negative");
    }
    this.table = new EEnum.Value[maxValue + 1];
  }

  public T valueOf(int value) {
    Value enu;
    try {
      enu = table[value];
    } catch (Exception exception) {
      enu = null;
    }
    if (enu == null) {
      throw new IllegalArgumentException("No enum with value " + value);
    }
    return (T) enu;
  }
}
