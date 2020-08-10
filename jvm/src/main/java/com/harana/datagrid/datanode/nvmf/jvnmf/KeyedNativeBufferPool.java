package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public interface KeyedNativeBufferPool {

  KeyedNativeBuffer allocate() throws IOException;
}
