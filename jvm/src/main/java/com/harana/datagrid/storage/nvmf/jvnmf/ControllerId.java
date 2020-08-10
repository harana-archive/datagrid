package com.harana.datagrid.storage.nvmf.jvnmf;

public class ControllerId {

  private final short id;

  public static final ControllerId ADMIN_DYNAMIC = new ControllerId((short) 0xFFFF);
  public static final ControllerId ADMIN_STATIC = new ControllerId((short) 0xFFFE);


  public ControllerId(short id) {
    this.id = id;
  }

  public short toShort() {
    return id;
  }

  public static ControllerId valueOf(short id) {
    if (id == ADMIN_STATIC.toShort()) {
      return ADMIN_STATIC;
    } else if (id == ADMIN_DYNAMIC.toShort()) {
      return ADMIN_DYNAMIC;
    } else {
      return new ControllerId(id);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    ControllerId that = (ControllerId) obj;

    return id == that.id;
  }

  @Override
  public int hashCode() {
    return (int) id;
  }
}
