package org.cloudata.core.common;

public class DefaultPerformanceRegulator implements PerformanceRegulator {
  final long limit;
  final long sleepTime;
  
  long totalNumWritten = 0;
  long totalNumRead = 0;
  
  public DefaultPerformanceRegulator(long limit, long sleepTime) {
    this.limit = limit;
    this.sleepTime = sleepTime;
  }

  public void recordRead(int numRead) {
    totalNumRead += numRead;
    
    if (totalNumRead >= limit) {
      try {
        Thread.sleep(sleepTime);
        totalNumRead = 0;
      } catch(InterruptedException e) {
      }
    }
  }

  public void recordWrite(int numWritten) {
    totalNumWritten += numWritten;
    
    if (totalNumWritten >= limit) {
      try {
        Thread.sleep(sleepTime);
        totalNumWritten = 0;
      } catch(InterruptedException e) {
      }
    }    
  }
}
