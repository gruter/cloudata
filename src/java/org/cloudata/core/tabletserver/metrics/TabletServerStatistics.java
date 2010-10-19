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

import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

import org.cloudata.core.common.metrics.CloudataMetricsFactory;


/**
 * @author jindolk
 *
 */
public class TabletServerStatistics implements TabletServerStatisticsMBean {
  TabletServerMetrics metrics;
  
  ObjectName mbeanName;
  StandardMBean bean;
  
  public TabletServerStatistics(TabletServerMetrics metrics) {
    this.metrics = metrics;
    try {
      bean = new StandardMBean(this, TabletServerStatisticsMBean.class);
    } catch (NotCompliantMBeanException e) {
      e.printStackTrace();
    }
    mbeanName = CloudataMetricsFactory.getFactory().registerMBean("Cloudata", metrics.metricsName, bean);
  }

  @Override
  public double getAvgGetTime() {
    return metrics.getTime.getAvg();
  }

  @Override
  public double getAvgPutTime() {
    return metrics.putTime.getAvg();
  }

  @Override
  public double getAvgQueueWaitTime() {
    return metrics.queueWaitTime.getAvg();
  }

  @Override
  public long getCallQueue() {
    return metrics.callQueue.get();
  }

  @Override
  public long getMaxGetTime() {
    return metrics.getTime.getMax();
  }

  @Override
  public long getMaxMajorCompactionTime() {
    return metrics.majorCompactionTime.getMax();
  }

  @Override
  public long getMaxMinorCompactionTime() {
    return metrics.minorCompactionTime.getMax();
  }

  @Override
  public long getMaxPutTime() {
    return metrics.putTime.getMax();
  }

  @Override
  public long getMaxQueueWaitTime() {
    return metrics.queueWaitTime.getMax();
  }

  @Override
  public long getMaxSplitFinishTime() {
    return metrics.splitFinishTime.getMax();
  }

  @Override
  public long getMaxSplitTime() {
    return metrics.splitTime.getMax();
  }

  @Override
  public long getNumGet() {
    return metrics.getNum.getDiff();
  }

  @Override
  public int getNumHeaveMemory() {
    return metrics.numHeaveMemory.get();
  }

  @Override
  public int getNumLoadedTablet() {
    return metrics.numLoadedTablet.get();
  }

  @Override
  public int getNumMajorCompaction() {
    return (int)metrics.majorCompactionTime.getCount();
  }

  @Override
  public int getNumMinorCompaction() {
    return (int)metrics.minorCompactionTime.getCount();
  }

  @Override
  public long getNumPut() {
    return metrics.putNum.getDiff();
  }

  @Override
  public long getNumRunningCall() {
    return metrics.runningCall.get();
  }

  @Override
  public long getNumScannerOpen() {
    return metrics.numScannerOpen.get();
  }

  @Override
  public int getNumSplit() {
    return (int)metrics.splitTime.getCount();
  }

  @Override
  public long getPutBytes() {
    return metrics.putBytes.getDiff();
  }

  @Override
  public long getSumPutBytes() {
    return metrics.putBytes.getValue();
  }
  
  @Override
  public long getGetBytes() {
    return metrics.getBytes.getDiff();
  }

  @Override
  public long getSumGetBytes() {
    return metrics.getBytes.getValue();
  }
  
  @Override
  public long getPutTime() {
    return metrics.putTime.getDiff();
  }
  
  @Override
  public long getGetTime() {
    return metrics.getTime.getDiff();
  }
}
