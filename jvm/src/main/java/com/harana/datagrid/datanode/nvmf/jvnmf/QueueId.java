package com.harana.datagrid.datanode.nvmf.jvnmf;

public final class QueueId {

  private final short id;

  public static final QueueId ADMIN = new QueueId((short) 0);

  public QueueId(short id) {
    this.id = id;
  }

  public short toShort() {
    return id;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    QueueId queueId = (QueueId) obj;

    return id == queueId.id;
  }

  @Override
  public int hashCode() {
    return (int) id;
  }

  @Override
  public String toString() {
    return Short.toString(id);
  }
}
