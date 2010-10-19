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
package org.cloudata.core.master.metrics;

import java.util.concurrent.atomic.AtomicInteger;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.metrics.JvmMetrics;
import org.cloudata.core.common.metrics.MetricsUpdater;
import org.cloudata.core.common.metrics.MetricsValue;
import org.cloudata.core.common.metrics.CloudataMetricsContext;
import org.cloudata.core.common.metrics.CloudataMetricsFactory;


/**
 * @author jindolk
 *
 */
public class CloudataMasterMetrics implements MetricsUpdater {
  private static final String CONTEXT_NAME = "CloudataMaster";

  JvmMetrics jvmMetrics;

  AtomicInteger liveTabletServerNum = new AtomicInteger(0);
  AtomicInteger deadTabletServerNum = new AtomicInteger(0);
  AtomicInteger liveCommitLogServerNum = new AtomicInteger(0);
  AtomicInteger deadCommitLogServerNum = new AtomicInteger(0);

  AtomicInteger totalTabletNum = new AtomicInteger(0);

  int interval;
  
  String metricsName;
  
  public CloudataMasterMetrics(CloudataConf conf) {
    metricsName = "_" + CONTEXT_NAME;
    jvmMetrics = new JvmMetrics(metricsName);
    
    CloudataMetricsContext context = CloudataMetricsFactory.getFactory().getContext(metricsName, CONTEXT_NAME);
    context.registerMetricsUpdater(this);
  }
  

  public void shutdown() {
    jvmMetrics.shutdown();
  }
  
  public void doUpdates(CloudataMetricsContext context) {
    context.clearMetricsData(metricsName);
    
    AtomicInteger liveTabletServerNum = new AtomicInteger(0);
    AtomicInteger deadTabletServerNum = new AtomicInteger(0);
    AtomicInteger liveCommitLogServerNum = new AtomicInteger(0);
    AtomicInteger deadCommitLogServerNum = new AtomicInteger(0);

    AtomicInteger totalTabletNum = new AtomicInteger(0);
    
    context.addMetricsData(metricsName, new MetricsValue("LiveTabletServer", new Integer(liveTabletServerNum.get())));  
    context.addMetricsData(metricsName, new MetricsValue("DeadTabletServer", new Integer(deadTabletServerNum.get())));  
    context.addMetricsData(metricsName, new MetricsValue("LiveCommitLogServer", new Integer(liveCommitLogServerNum.get())));  
    context.addMetricsData(metricsName, new MetricsValue("DeadCommitLogServer", new Integer(deadCommitLogServerNum.get())));  
    context.addMetricsData(metricsName, new MetricsValue("Tablets", new Integer(totalTabletNum.get())));  
  }

  public void setJvmMetrics(JvmMetrics jvmMetrics) {
    this.jvmMetrics = jvmMetrics;
  }

  public void setLiveTabletServerNum(int liveTabletServerNum) {
    this.liveTabletServerNum.set(liveTabletServerNum);
  }

  public void setDeadTabletServerNum(int deadTabletServerNum) {
    this.deadTabletServerNum.set(deadTabletServerNum);
  }

  public void setLiveCommitLogServerNum(int liveCommitLogServerNum) {
    this.liveCommitLogServerNum.set(liveCommitLogServerNum);
  }

  public void setDeadCommitLogServerNum(int deadCommitLogServerNum) {
    this.deadCommitLogServerNum.set(deadCommitLogServerNum);
  }

  public void setTotalTabletNum(int totalTabletNum) {
    this.totalTabletNum.set(totalTabletNum);
  }

  public void setInterval(int interval) {
    this.interval = interval;
  }


  public void setMetricsName(String metricsName) {
    this.metricsName = metricsName;
  }

}
