/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.cloudata.core.tabletserver.metrics;


/**
 * @author jindolk
 *
 */
public interface TabletServerStatisticsMBean {
  public long getNumPut();
  public long getNumGet();
  public double getAvgPutTime();
  public double getAvgGetTime();
  public long getMaxPutTime();
  public long getMaxGetTime();
  public long getPutBytes();
  public long getSumPutBytes();
  public long getGetBytes();
  public long getSumGetBytes();
  
  public long getNumRunningCall();
  public long getCallQueue();
  public double getAvgQueueWaitTime();
  public long getMaxQueueWaitTime();
  
  public int getNumLoadedTablet();
  public int getNumMinorCompaction();
  public int getNumMajorCompaction();
  public int getNumSplit();
  
  public long getMaxMinorCompactionTime();
  public long getMaxMajorCompactionTime();
  public long getMaxSplitTime();
  public long getMaxSplitFinishTime();

  public int getNumHeaveMemory();
  public long getNumScannerOpen();
  
  public long getPutTime();
  public long getGetTime();
}
