package com.harana.datagrid.datanode.nvmf.jvnmf;

import java.io.IOException;

public abstract class Updatable<T> {

  /* we don't use an interface since we want this to be package private */
  abstract void update(T arg) throws IOException;
}
