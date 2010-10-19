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
package org.cloudata.core.common.metrics;

import static java.lang.Thread.State.BLOCKED;
import static java.lang.Thread.State.NEW;
import static java.lang.Thread.State.RUNNABLE;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;

import org.cloudata.core.common.metrics.MetricsValue.MetricsValueSeperator;


/**
 * @author jindolk
 *
 */
public class JvmMetrics implements MetricsUpdater {
  private static final float M = 1024*1024;
  
  // garbage collection counters
  private long gcCount = 0;
  private long gcTimeMillis = 0;
  
  private long cpuTime = 0;
  
  private String jvmMetricsName;
  
  private CloudataMetricsContext context;
  
  public JvmMetrics(String metricsName) {
    context = CloudataMetricsFactory.getFactory().getContext(metricsName, "jvm");
    context.registerMetricsUpdater(this);
    
    jvmMetricsName = metricsName + ".jvm";
  }
  
  public void doUpdates(CloudataMetricsContext context) {
    context.clearMetricsData(jvmMetricsName);
    
    doOSUpdates(context);
    doMemoryUpdates(context);
    doGarbageCollectionUpdates(context);
    context.addMetricsData(jvmMetricsName, new MetricsValueSeperator());
    doThreadUpdates(context);
  }
  
  private void doOSUpdates(CloudataMetricsContext context) {
//    OperatingSystemMXBean osMXBean = (OperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
    
//    long currentCpuTime = osMXBean.getProcessCpuTime();
//    
//    context.addMetricsData(jvmMetricsName, new MetricsValue("SystemLoadAverage", osMXBean.getSystemLoadAverage()));
//    context.addMetricsData(jvmMetricsName, new MetricsValue("CpuTime(sec)", osMXBean.getProcessCpuTime() / 1000));
//    context.addMetricsData(jvmMetricsName, new MetricsValue("CpuTimeGap", (currentCpuTime - cpuTime)/1000));
//    
//    this.cpuTime = currentCpuTime;

    RuntimeMXBean runbean = (RuntimeMXBean) ManagementFactory.getRuntimeMXBean();
    context.addMetricsData(jvmMetricsName, new MetricsValue("Uptime", runbean.getUptime()));
  }
  
  private void doMemoryUpdates(CloudataMetricsContext context) {
    MemoryMXBean memoryMXBean =
           ManagementFactory.getMemoryMXBean();
    MemoryUsage memNonHeap =
            memoryMXBean.getNonHeapMemoryUsage();
    MemoryUsage memHeap =
            memoryMXBean.getHeapMemoryUsage();
    
    context.addMetricsData(jvmMetricsName, new MetricsValue("HeapUsedM", memHeap.getUsed()/M));
    context.addMetricsData(jvmMetricsName, new MetricsValue("HeapCommittedM", memHeap.getCommitted()/M));
    context.addMetricsData(jvmMetricsName, new MetricsValue("NonHeapUsedM", memNonHeap.getUsed()/M));
    context.addMetricsData(jvmMetricsName, new MetricsValue("NonHeapCommittedM", memNonHeap.getCommitted()/M));
  }

  private void doGarbageCollectionUpdates(CloudataMetricsContext context) {
    List<GarbageCollectorMXBean> gcBeans =
            ManagementFactory.getGarbageCollectorMXBeans();
    long count = 0;
    long timeMillis = 0;
    for (GarbageCollectorMXBean gcBean : gcBeans) {
        count += gcBean.getCollectionCount();
        timeMillis += gcBean.getCollectionTime();
    }
    context.addMetricsData(jvmMetricsName, new MetricsValue("gcCount", (int)(count - gcCount)));
    context.addMetricsData(jvmMetricsName, new MetricsValue("gcTimeMillis", (int)(timeMillis - gcTimeMillis)));
    
    gcCount = count;
    gcTimeMillis = timeMillis;
  }
  
  private void doThreadUpdates(CloudataMetricsContext context) {
    ThreadMXBean threadMXBean =
            ManagementFactory.getThreadMXBean();
    long threadIds[] = 
            threadMXBean.getAllThreadIds();
    ThreadInfo[] threadInfos =
            threadMXBean.getThreadInfo(threadIds, 0);
    
    int threadsNew = 0;
    int threadsRunnable = 0;
    int threadsBlocked = 0;
    int threadsWaiting = 0;
    int threadsTimedWaiting = 0;
    int threadsTerminated = 0;
    
    for (ThreadInfo threadInfo : threadInfos) {
        // threadInfo is null if the thread is not alive or doesn't exist
        if (threadInfo == null) continue;
        Thread.State state = threadInfo.getThreadState();
        if (state == NEW) {
            threadsNew++;
        } 
        else if (state == RUNNABLE) {
            threadsRunnable++;
        }
        else if (state == BLOCKED) {
            threadsBlocked++;
        }
        else if (state == WAITING) {
            threadsWaiting++;
        } 
        else if (state == TIMED_WAITING) {
            threadsTimedWaiting++;
        }
        else if (state == TERMINATED) {
            threadsTerminated++;
        }
    }
    context.addMetricsData(jvmMetricsName, new MetricsValue("threadsNew", threadsNew));
    context.addMetricsData(jvmMetricsName, new MetricsValue("threadsRunnable", threadsRunnable));
    context.addMetricsData(jvmMetricsName, new MetricsValue("threadsBlocked", threadsBlocked));
    context.addMetricsData(jvmMetricsName, new MetricsValue("threadsWaiting", threadsWaiting));
    context.addMetricsData(jvmMetricsName, new MetricsValue("threadsTimedWaiting", threadsTimedWaiting));
    context.addMetricsData(jvmMetricsName, new MetricsValue("threadsTerminated", threadsTerminated));
  }  
  
  public void shutdown() {
    context.stopMonitoring();
  }
}
