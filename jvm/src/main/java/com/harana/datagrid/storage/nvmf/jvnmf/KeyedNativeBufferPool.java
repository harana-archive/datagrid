package com.harana.datagrid.storage.nvmf.jvnmf;

import java.io.IOException;

public interface KeyedNativeBufferPool {

  KeyedNativeBuffer allocate() throws IOException;
}
