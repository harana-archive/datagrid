package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public interface Freeable {

  void free() throws IOException;

  boolean isValid();
}
