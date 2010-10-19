package org.cloudata.core.common;

import org.apache.commons.logging.Log;

public class CStopWatch {
  private final static boolean ACTIVATED = true; 
  private int expectedTime;
  private long startTime;
  private String regionName;

  public CStopWatch() {
  }

  public void start(String regionName, int expectedTime) {
    this.regionName = regionName;
    this.expectedTime = expectedTime;
    this.startTime = System.currentTimeMillis();
  }

  public long stopAndGet() {
    return System.currentTimeMillis() - startTime;
  }

  public void stopAndReportIfExceed(Log LOG) {
    long t = System.currentTimeMillis();
    
    if (ACTIVATED && t - startTime > expectedTime) {
      LOG.info("TIME REPORT, [" + regionName + "] takes " + (t - startTime) + "ms, but (" + expectedTime + ").");
    }
  }
}
