package com.harana.datagrid.storage.nvmf.jvnmf;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ControllerIdTest {

  @Test
  void constants() {
    ControllerId id = ControllerId.valueOf(ControllerId.ADMIN_DYNAMIC.toShort());
    assertEquals(ControllerId.ADMIN_DYNAMIC, id);
    id = ControllerId.valueOf(ControllerId.ADMIN_STATIC.toShort());
    assertEquals(ControllerId.ADMIN_STATIC, id);
  }

  @Test
  void equals() {
    ControllerId id = ControllerId.valueOf((short) 0x123);
    ControllerId id2 = ControllerId.valueOf((short) 0x123);
    assertEquals(id, id2);
  }
}