package com.harana.datagrid.datanode.nvmf.jnvmf;

import java.io.IOException;

public interface KeyedNativeBufferPool {

  KeyedNativeBuffer allocate() throws IOException;
}
