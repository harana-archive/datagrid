package com.harana.datagrid.storage.nvmf.jvnmf;

import java.util.Objects;
import java.util.UUID;

public class NvmeQualifiedName {
  /*
   * Nvme Spec 1.3a - 7.9
   *
   * UTF-8 encoded string of format:
   * 1) nqn.yyyy-mm.domain.name:any-name
   * 2) nqn.2014-08.org.nvmexpress:uuid:128bitUUID (RFC4122)
   *
   */

  private final String nqn;

  public NvmeQualifiedName(String nqn) {
    validate(nqn);
    this.nqn = nqn;
  }

  public NvmeQualifiedName(UUID uuid) {
    this.nqn = "nqn.2014-08.org.nvmexpress:uuid:" + uuid.toString();
  }

  private static void validate(String nqn) {
    /* we only care about format 1 for now */
    if (!nqn.matches("nqn\\.\\d{4}-\\d{2}\\.[a-zA-Z0-9.-]*:.*")) {
      throw new IllegalArgumentException("Invalid NQN");
    }
  }

  @Override
  public String toString() {
    return nqn;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    NvmeQualifiedName that = (NvmeQualifiedName) obj;
    return Objects.equals(this.nqn, that.nqn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nqn);
  }
}
