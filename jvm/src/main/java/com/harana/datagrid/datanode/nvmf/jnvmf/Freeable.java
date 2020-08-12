package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public interface Freeable {

  void free() throws IOException;

  boolean isValid();
}
