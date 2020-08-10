package com.harana.datagrid.datanode.nvmf.jvnmf;

public class NamespaceIdentifierList extends NativeData<KeyedNativeBuffer> {

  static final int SIZE = 4096;

  NamespaceIdentifierList(KeyedNativeBuffer buffer) {
    super(buffer, SIZE);
  }

  NamespaceIdentifier getIdentifier(int index) {
    if (index < 0 || index >= (SIZE / NamespaceIdentifier.SIZE)) {
      throw new IllegalArgumentException("invalid index");
    }
    int namespaceId = getBuffer().getInt(index * NamespaceIdentifier.SIZE);
    if (namespaceId == 0) {
      return null;
    }
    return new NamespaceIdentifier(namespaceId);
  }

  @Override
  void initialize() {

  }
}
