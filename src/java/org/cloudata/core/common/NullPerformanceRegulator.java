package org.cloudata.core.common;

public class NullPerformanceRegulator implements PerformanceRegulator {
  public void recordRead(int numRead) {
  }

  public void recordWrite(int numWritten) {
  }
}
