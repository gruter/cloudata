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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.metrics.DummyMetricContext;
import org.cloudata.core.common.metrics.JvmMetrics;
import org.cloudata.core.common.metrics.MetricsNumber;
import org.cloudata.core.common.metrics.MetricsUpdater;
import org.cloudata.core.common.metrics.MetricsValue;
import org.cloudata.core.common.metrics.CloudataMetricsContext;
import org.cloudata.core.common.metrics.CloudataMetricsFactory;
import org.cloudata.core.common.metrics.RPCServerMetrics;
import org.cloudata.core.common.metrics.SystemMetrics;
import org.cloudata.core.common.metrics.MetricsValue.MetricsValueSeperator;
import org.cloudata.core.tabletserver.TabletServer;


/**
 * @author jindolk
 *
 */
public class TabletServerMetrics implements MetricsUpdater, RPCServerMetrics {
  public static final int TYPE_PUT = 1;
  public static final int TYPE_PUT_FAIL = 2;
  public static final int TYPE_PUT_SKIP = 3;
  public static final int TYPE_GET = 4;
  
  static final double HUNDRED_THOUSAND = 100000.0;
  static final double MILLION = 1000000.0;
  
  private static final String CONTEXT_NAME = "TabletServer";

  final String[] medianString = {
      "0.0~0.1", "0.1~0.2", "0.2~0.3", "0.3~0.4", 
      "0.4~0.5", "0.5~0.6", "0.6~0.7", "0.7~0.8", 
      "0.8~0.9", "0.9~1.0", "1~2", "2~3", 
      "3~4", "4~5", "5~6", "6~7", 
      "7~8", "8~9", "9~10", "10~100", 
      "100~1000", "1000~"
    };
  
  TabletServerStatistics statistics;
  JvmMetrics jvmMetrics;
  SystemMetrics systemMetrics;

  long lastTime;

  MetricsNumber putNum = new MetricsNumber();
  MetricsNumber putTime = new MetricsNumber();
  MetricsNumber putBytes = new MetricsNumber();
  MetricsNumber putSkipNum = new MetricsNumber();
  MetricsNumber putFailNum = new MetricsNumber();

  MetricsNumber getNum = new MetricsNumber();
  MetricsNumber getTime = new MetricsNumber();
  MetricsNumber getBytes = new MetricsNumber();

  //////////////////////////////////////////////////////////////////////////////
  //Transaction
  DecimalFormat format = new DecimalFormat("###,###.##");
  DecimalFormat format2 = new DecimalFormat("###,###,###,###,###");
  List<TimeStatistics> timeStatistics = new ArrayList<TimeStatistics>();  
  
  Object putLock = new Object();
  Object getLock = new Object();
  int[] medianRange = new int[22]; 
  long lastTxStatiticsTimestamp;
  int interval;
  //////////////////////////////////////////////////////////////////////////////
  
  //////////////////////////////////////////////////////////////////////////////
  //RPC
  AtomicLong runningCall = new AtomicLong(0);
  AtomicLong callQueue = new AtomicLong(0);
  MetricsNumber queueWaitTime = new MetricsNumber();
  //////////////////////////////////////////////////////////////////////////////
  
  //////////////////////////////////////////////////////////////////////////////
  //Tablet management
  public AtomicInteger numLoadedTablet = new AtomicInteger(0);

  MetricsNumber minorCompactionTime = new MetricsNumber();
  MetricsNumber majorCompactionTime = new MetricsNumber();
  MetricsNumber splitTime = new MetricsNumber();
  MetricsNumber splitFinishTime = new MetricsNumber();
  
  public AtomicInteger numHeaveMemory = new AtomicInteger(0);

  public AtomicLong numScannerOpen = new AtomicLong(0);
  //////////////////////////////////////////////////////////////////////////////

  private TabletServer tabletServer;
  
  String metricsName;
  
  public TabletServerMetrics(CloudataConf conf, TabletServer tabletServer) {
    if(tabletServer != null) {
      metricsName = tabletServer.getHostName().replace(":", "_") + "_" + CONTEXT_NAME;
    } else {
      metricsName = "_" + CONTEXT_NAME;
    }
    interval = conf.getInt("tabletServer.tx.statistics.interval", 10) * 60 * 1000;
    lastTxStatiticsTimestamp = System.currentTimeMillis();
    
    jvmMetrics = new JvmMetrics(metricsName);
    systemMetrics = new SystemMetrics(metricsName);
    
    statistics = new TabletServerStatistics(this);
    CloudataMetricsContext context = CloudataMetricsFactory.getFactory().getContext(metricsName, CONTEXT_NAME);
    context.registerMetricsUpdater(this);
    
    this.tabletServer = tabletServer;
  }
  

  public void shutdown() {
    jvmMetrics.shutdown();
    systemMetrics.shutdown();
  }
  
  public void setMinorCompactionTime(long time) {
    minorCompactionTime.add(time);
  }
  
  public void setMajorCompactionTime(long time) {
    majorCompactionTime.add(time);
  }
  
  public void setSplitTime(long time) {
    splitTime.add(time);
  }
  
  public void setSplitFinishTime(long time) {
    splitFinishTime.add(time);
  }
  
  public void doUpdates(CloudataMetricsContext context) {
    context.clearMetricsData(metricsName);
    
    long currentTime = System.currentTimeMillis();
    
    if(currentTime - lastTxStatiticsTimestamp >= interval) {
      lastTxStatiticsTimestamp = currentTime;

      TimeStatistics ts = makeTxStatics();
      if(ts != null) {
        context.addMetricsData(metricsName, new MetricsValue("transaction", ts.toString()));
        context.addMetricsData(metricsName, new MetricsValueSeperator());
      }
    }
    
    long elapsedTime = currentTime - lastTime;
    lastTime = currentTime;
    
    context.addMetricsData(metricsName, new MetricsValue("Put_Num", putNum.getDiff() + "," + putNum.getValue()));  
    if(elapsedTime > 0) {
      context.addMetricsData(metricsName, new MetricsValue("Put_TPS", new Float(putNum.getDiff() / elapsedTime)));
    }
    context.addMetricsData(metricsName, new MetricsValue("Put_Time", putTime.getDiff() + "," + putNum.getValue()));   
    context.addMetricsData(metricsName, new MetricsValue("Put_AvgTime", new Float(putTime.getAvg())));   
    context.addMetricsData(metricsName, new MetricsValue("Put_MaxTime", new Float(putTime.getMax())));   
    context.addMetricsData(metricsName, new MetricsValue("Put_Bytes", putBytes.getDiff() + "," + putBytes.getValue()));   
    if(elapsedTime > 0) {
      context.addMetricsData(metricsName, new MetricsValue("Put_Rate", new Float(putBytes.getDiff() / elapsedTime)));      
    }
    context.addMetricsData(metricsName, new MetricsValue("Put_Fail_Num", putFailNum.getDiff() + "," + putFailNum.getValue())); 
    context.addMetricsData(metricsName, new MetricsValue("Put_Skip_Num", putSkipNum.getDiff() + "," + putSkipNum.getValue()));      
    context.addMetricsData(metricsName, new MetricsValueSeperator());      
    
    putNum.mark();
    putTime.mark();
    putBytes.mark();
    putSkipNum.mark();
    putFailNum.mark();
    
    context.addMetricsData(metricsName, new MetricsValue("Get_Num", getNum.getDiff() + "," + getNum.getValue()));  
    if(elapsedTime > 0) {
      context.addMetricsData(metricsName, new MetricsValue("Get_TPS", new Float(getNum.getDiff() / elapsedTime)));  
    }
    context.addMetricsData(metricsName, new MetricsValue("Get_Time", getNum.getDiff() + "," + getNum.getValue()));   
    context.addMetricsData(metricsName, new MetricsValue("Get_AvgTime", getNum.getAvg()));   
    context.addMetricsData(metricsName, new MetricsValue("Get_MaxTime", getNum.getMax()));   
    context.addMetricsData(metricsName, new MetricsValue("Get_Bytes", getBytes.getDiff() + "," + getBytes.getValue()));   
    if(elapsedTime > 0) {
      context.addMetricsData(metricsName, new MetricsValue("Get_Rate", new Float(getBytes.getDiff() / elapsedTime)));
    }
    context.addMetricsData(metricsName, new MetricsValueSeperator());      

    getNum.mark();
    getTime.mark();
    getBytes.mark();
    
    context.addMetricsData(metricsName, new MetricsValue("NumMinor", minorCompactionTime.getCount()));
    context.addMetricsData(metricsName, new MetricsValue("AvgTime", minorCompactionTime.getAvg()));
    context.addMetricsData(metricsName, new MetricsValue("MaxTime", minorCompactionTime.getMax()));
    context.addMetricsData(metricsName, new MetricsValue("NumMajor", majorCompactionTime.getCount()));
    context.addMetricsData(metricsName, new MetricsValue("AvgTime", majorCompactionTime.getAvg()));
    context.addMetricsData(metricsName, new MetricsValue("MaxTime", majorCompactionTime.getMax()));
    context.addMetricsData(metricsName, new MetricsValue("NumSplit", splitTime.getCount()));
    context.addMetricsData(metricsName, new MetricsValue("AvgTime", splitTime.getAvg()));
    context.addMetricsData(metricsName, new MetricsValue("MaxTime", splitTime.getMax()));
    context.addMetricsData(metricsName, new MetricsValue("NumSplitF", splitFinishTime.getCount()));
    context.addMetricsData(metricsName, new MetricsValue("AvgTime", splitFinishTime.getAvg()));
    context.addMetricsData(metricsName, new MetricsValue("MaxTime", splitFinishTime.getMax()));
    context.addMetricsData(metricsName, new MetricsValueSeperator());      

    context.addMetricsData(metricsName, new MetricsValue("LoadedTablet", numLoadedTablet));
    if(tabletServer != null) {
      context.addMetricsData(metricsName, new MetricsValue("ServiceTablet", tabletServer.getTabletCount()));
    }
    context.addMetricsData(metricsName, new MetricsValue("RunningCall", runningCall));
    context.addMetricsData(metricsName, new MetricsValue("CallQueue", callQueue));
    context.addMetricsData(metricsName, new MetricsValue("AvgQueueWaitTime", queueWaitTime.getAvg()));
    context.addMetricsData(metricsName, new MetricsValue("maxQueueWaitTime", queueWaitTime.getMax()));
    context.addMetricsData(metricsName, new MetricsValue("numHeaveMemory", numHeaveMemory));
    context.addMetricsData(metricsName, new MetricsValue("numScannerOpen", numScannerOpen));
    context.addMetricsData(metricsName, new MetricsValueSeperator());      

    queueWaitTime.mark();
    minorCompactionTime.mark();
    majorCompactionTime.mark();
    splitTime.mark();
    splitFinishTime.mark();
    
    numHeaveMemory.set(0);
  }

  public void incrementRunningCall() {
    runningCall.incrementAndGet();
  }
  public void decrementRunningCall() {
    runningCall.decrementAndGet();
  }
  
  public void incrementCallQueue() {
    callQueue.incrementAndGet();
  }
  public void decrementCallQueue() {
    callQueue.decrementAndGet();
  }
  
  public void setQueueWaitTime(long time) {
    queueWaitTime.add(time);
  }
  
  public void addTxByte(int txType, long bytes) {
    //sumPutBytes += bytes;
    if(txType == TYPE_GET) {
      getBytes.add(bytes);
    } else if(txType == TYPE_PUT) {
      putBytes.add(bytes);
    }
  }
  
  public void addTx(int txType, long elapsedTime) {
    if(txType == TYPE_GET) {
      getNum.add(1);
      getTime.add((long)(elapsedTime / MILLION));
      return;
    } 
    
    long elapsed = (long)(elapsedTime / MILLION);

    if(txType == TYPE_PUT) {
      //collectMedian(elapsedTime);
      putNum.add(1);
      putTime.add(elapsed);
    } else if(txType == TYPE_PUT_FAIL) {
      putFailNum.add(1);
    } else if(txType == TYPE_PUT_SKIP) {
      putSkipNum.add(1);
    }
  }
  
  private TimeStatistics makeTxStatics() {
    TimeStatistics st = new TimeStatistics(System.currentTimeMillis());
    
    TimeStatistics previous;
    if(timeStatistics.size() > 0) {
      previous = timeStatistics.get(timeStatistics.size() - 1);
    } else {
      previous = new TimeStatistics(System.currentTimeMillis());
    }
    
    synchronized(getLock) {
      st.getNum = getNum.getValue() - previous.totalGetNum;
      st.getTime = getTime.getValue() - previous.totalGetTime;
      st.getBytes = getBytes.getValue() - previous.getBytes;

      st.totalGetNum = getNum.getValue();
      st.totalGetTime = getTime.getValue();
      st.totalGetBytes = getBytes.getValue();
    }

    synchronized(putLock) {
      st.putMedian = getMedian(putNum.getValue() - previous.totalPutNum); 
      st.putNum = putNum.getValue() - previous.totalPutNum;
      st.putTime = putTime.getValue() - previous.totalPutTime;
      st.putBytes = putBytes.getValue() - previous.totalPutBytes;
      st.putFailNum = putFailNum.getValue() - previous.totalPutFailNum;
      st.putSkipNum = putSkipNum.getValue() - previous.totalPutSkipNum;

      st.totalPutNum = putNum.getValue();
      st.totalPutTime = putTime.getValue();
      st.totalPutBytes = putBytes.getValue();
      st.totalPutFailNum = putFailNum.getValue();
      st.totalPutSkipNum = putSkipNum.getValue();
      
      clearMedian();
    }

    if (st.getNum > 0 || st.putNum > 0) {
      synchronized(timeStatistics) {
        if(timeStatistics.size() > 5000) {
          int removeNum = timeStatistics.size() - 5000;
          for(int i = 0; i < removeNum; i++) {
            timeStatistics.remove(0);
          }
        }
        //System.out.println("ADD>" + st.toString());
        timeStatistics.add(st);
        return st;
      }
    }
    return null;
  }
  
  public double getPutGetRatio() {
    synchronized(timeStatistics) {
      long totalNumGet = 0;
      long totalNumPut = 0;
      
    if (timeStatistics.size() >= 2) {
        for(int i = timeStatistics.size() - 2; i < timeStatistics.size(); i++) {
          TimeStatistics st = timeStatistics.get(i);
          
          if (System.currentTimeMillis() - st.dateTime > interval * 2) {
            return 0.0;
          }
          
          totalNumGet += st.getNum;
          totalNumPut += st.putNum;
        }

        if (totalNumPut + totalNumGet > 0) {
          return (double) totalNumPut / (double) (totalNumPut + totalNumGet);
        }
      }
      
      return 0.0;
    }
  }  
  
  private void clearMedian() {
    for(int i = 0; i < medianRange.length; i++) {
      medianRange[i] = 0;
    }
  }

  private String getMedian(long sumPut) {
    long halfSumPut = sumPut / 2;
    long currentSumPut = 0;
    for(int i = 0; i < medianRange.length; i++) {
      currentSumPut += medianRange[i];
      if (currentSumPut >= halfSumPut) {
        return medianString[i];
      }
    }
    
    return medianString[medianRange.length - 1];
  }
  
//  private void collectMedian(long elapsedTime) {
//    int index = elapsedTime < HUNDRED_THOUSAND
//      ? 0 : (int) (elapsedTime / HUNDRED_THOUSAND);
//    
//    if (index > 99) {
//      index = (int) Math.log10((double) elapsedTime) + 12;
//    } else if (index > 9) {
//      index = (int) (elapsedTime / MILLION) + 9;
//    }
//    
//    medianRange[index]++;
//  }
  
  class TimeStatistics {
    long putNum;
    long putTime;
    long putBytes;
    long putSkipNum;
    long putFailNum;

    long getNum;
    long getTime;
    long getBytes;
    
    long totalPutNum;
    long totalPutTime;
    long totalPutBytes;
    long totalPutSkipNum;
    long totalPutFailNum;

    long totalGetNum;
    long totalGetTime;
    long totalGetBytes;

    String putMedian = "";
    boolean type;     //true:get, false: put
    long dateTime;

    public TimeStatistics(long dateTime) {
      this.dateTime = dateTime;
    }

    public String toString() {
      String result = (new Date(dateTime)).toString();
      
      if(putNum > 0) {
//        result += (" [put: " +  format2.format(putTime) + "ms/" 
//              + putNum + " = " + format.format((double)putTime / (double)putNum) 
//              + " ms (" + putMedian + "), " + format2.format(putBytes/1024) +"KBytes], " 
//              + " [put retry: " + putFailNum  + "], " 
//              + " [put skip:" + putSkipNum + "]");

        result += (" [put: " +  format2.format(putTime) + "ms/" 
            + putNum + " = " + format.format((double)putTime / (double)putNum) 
            + " ms, " + format2.format(putBytes/1024) +" KB], " 
            + " [put retry: " + putFailNum  + "], " 
            + " [put skip:" + putSkipNum + "]");        
      } 
      if(getNum > 0) {
        result +=( "[get: " +  format2.format(getTime) + "/" + 
            getNum + " = " + format.format((double)getTime / (double)getNum) + " ms] ");
      }
      
      return result;
    }
  }

  public String[] getTxStatistics() {
    synchronized(timeStatistics) {
      if(timeStatistics.isEmpty()) {
        return new String[]{"No TxStatistics"};
      } else {
        List<String> result = new ArrayList<String>();
        for(TimeStatistics eachItem: timeStatistics) {
          result.add(eachItem.toString());
        }
        
        return result.toArray(new String[]{});
      }
    }
  }
  
  public static void main(String[] args) throws Exception {
    CloudataConf conf = new CloudataConf();
    TabletServerMetrics metrics = new TabletServerMetrics(conf, null);
    metrics.interval = 100;
    
    metrics.doUpdates(new DummyMetricContext());
    
    Thread.sleep(200);
    for(int i = 0; i < 50; i++) {
      metrics.addTx(TYPE_PUT, 10 * (long)MILLION);
      metrics.addTx(TYPE_PUT, 5 * (long)MILLION);
      metrics.addTxByte(TYPE_PUT, 1024 * 1024);
    }
    metrics.doUpdates(new DummyMetricContext());
    System.out.println(metrics.timeStatistics.get(0));
    
    Thread.sleep(200);
    for(int i = 0; i < 50; i++) {
      metrics.addTx(TYPE_PUT, 10 * (long)MILLION);
      metrics.addTx(TYPE_PUT, 5 * (long)MILLION);
      metrics.addTxByte(TYPE_PUT, 1024 * 1024);
    }
    metrics.doUpdates(new DummyMetricContext());
    System.out.println(metrics.timeStatistics.get(1));

    //System.out.println(metrics.timeStatistics.get(0));
    Thread.sleep(200);
    for(int i = 0; i < 50; i++) {
      metrics.addTx(TYPE_PUT, 10 * (long)MILLION);
      metrics.addTx(TYPE_PUT, 5 * (long)MILLION);
      metrics.addTxByte(TYPE_PUT, 1024 * 1024);
    }
    metrics.doUpdates(new DummyMetricContext());
    System.out.println(metrics.timeStatistics.get(2));
  }
}
