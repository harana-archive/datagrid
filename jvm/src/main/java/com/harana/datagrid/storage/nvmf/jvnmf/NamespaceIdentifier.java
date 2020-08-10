package com.harana.datagrid.storage.nvmf.jvnmf;

public final class NamespaceIdentifier {

  static final int SIZE = 4;

  private final int namespaceIdentifier;

  /*
   * NVMe Spec 1.3 - 4.2/6.1.2
   * Applies to all namespaces attached to the controller
   */
  public static final NamespaceIdentifier ALL = new NamespaceIdentifier(0xFFFFFFFF);

  public NamespaceIdentifier(int namespaceIdentifier) {
    this.namespaceIdentifier = namespaceIdentifier;
  }

  public int toInt() {
    return namespaceIdentifier;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    NamespaceIdentifier that = (NamespaceIdentifier) obj;

    return this.namespaceIdentifier == that.namespaceIdentifier;
  }

  @Override
  public int hashCode() {
    return namespaceIdentifier;
  }

  @Override
  public String toString() {
    return Integer.toString(namespaceIdentifier);
  }
}
