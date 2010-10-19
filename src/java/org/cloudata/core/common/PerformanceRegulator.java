package org.cloudata.core.common;

public interface PerformanceRegulator {
  public void recordRead(int numRead);
  public void recordWrite(int numWritten);
}
