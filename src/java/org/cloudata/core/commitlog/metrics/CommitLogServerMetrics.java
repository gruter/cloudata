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
package org.cloudata.core.commitlog.metrics;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.metrics.JvmMetrics;
import org.cloudata.core.common.metrics.MetricsNumber;
import org.cloudata.core.common.metrics.MetricsUpdater;
import org.cloudata.core.common.metrics.MetricsValue;
import org.cloudata.core.common.metrics.CloudataMetricsContext;
import org.cloudata.core.common.metrics.CloudataMetricsFactory;
import org.cloudata.core.common.metrics.SystemMetrics;
import org.cloudata.core.fs.DiskInfo;


/**
 * @author jindolk
 *
 */
public class CommitLogServerMetrics implements MetricsUpdater {
  private static final String CONTEXT_NAME = "CommitLogServer";

  JvmMetrics jvmMetrics;
  SystemMetrics systemMetrics;

  String metricsName;
  
  AtomicInteger currentTabletNum = new AtomicInteger(0);
  
  MetricsNumber writeNum = new MetricsNumber();
  MetricsNumber writeTime = new MetricsNumber();
  MetricsNumber writeBytes = new MetricsNumber();
  
  AtomicLong used = new AtomicLong(0);
  AtomicLong available = new AtomicLong(0);
  AtomicLong percentUsed = new AtomicLong(0);
  
  public CommitLogServerMetrics(CloudataConf conf, String hostName) {
    boolean onSystemMetrics = conf.getBoolean("commitLogServer.systemMetrics.on", false);
    metricsName = hostName.replace(":", "_") + "_" + CONTEXT_NAME;
    jvmMetrics = new JvmMetrics(metricsName);
    
    if(onSystemMetrics) {
      systemMetrics = new SystemMetrics(metricsName);
    }
    
    CloudataMetricsContext context = CloudataMetricsFactory.getFactory().getContext(metricsName, CONTEXT_NAME);
    context.registerMetricsUpdater(this);
  }
  

  public void shutdown() {
    jvmMetrics.shutdown();
    systemMetrics.shutdown();
  }
  
  public void doUpdates(CloudataMetricsContext context) {
    context.clearMetricsData(metricsName);
    
    context.addMetricsData(metricsName, new MetricsValue("TabletNum", new Integer(currentTabletNum.get())));   
    context.addMetricsData(metricsName, new MetricsValue("DiskUsed", new Long(used.get())));   
    context.addMetricsData(metricsName, new MetricsValue("DiskAvailable", new Long(available.get())));   
    context.addMetricsData(metricsName, new MetricsValue("DiskPercentUsed", new Long(percentUsed.get())));   

    context.addMetricsData(metricsName, new MetricsValue("WriteNum", writeNum.getDiff() + "," + writeNum.getValue()));  
    context.addMetricsData(metricsName, new MetricsValue("WriteTime", writeTime.getDiff() + "," + writeTime.getValue()));   
    context.addMetricsData(metricsName, new MetricsValue("WriteAvgTime", writeTime.getAvg()));   
    context.addMetricsData(metricsName, new MetricsValue("WriteMaxTime", writeTime.getMax()));   
    context.addMetricsData(metricsName, new MetricsValue("WriteBytes", writeBytes.getDiff() + "," + writeBytes.getValue())); 
    
    writeNum.mark();
    writeTime.mark();
    writeBytes.mark();
  }
  
  public void setCurrentTabletNum(int num) {
    currentTabletNum.set(num);
  }
  
  public void setWrite(long bytes, long time) {
    writeBytes.add(bytes);
    writeNum.add(1);
    writeTime.add(time);
  }

  public void setDiskInfo(DiskInfo diskInfo) {
    used.set(diskInfo.getUsed());
    available.set(diskInfo.getAvailable());
    percentUsed.set(diskInfo.getPercentUsed());
  }
}
