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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.cloudata.core.common.metrics.MetricsValue.MetricsValueSeperator;
import org.cloudata.core.common.metrics.system.SystemMetricsParser;


/**
 * @author jindolk
 *
 */
public class SystemMetrics implements MetricsUpdater {
  private String metricsName;
  private boolean existsFile = false;
  
  //file name -> SystemMetricsParser
  private Map<String, SystemMetricsParser> systemMetircsFiles = new HashMap<String, SystemMetricsParser>();
  
  private CloudataMetricsContext context;
  
  public SystemMetrics(String metricsName) {
    this.metricsName = metricsName;
    context = CloudataMetricsFactory.getFactory().getContext(metricsName, "system");
    context.registerMetricsUpdater(this);
    
    CloudataMetricsFactory factory = CloudataMetricsFactory.getFactory();
    String systemFileNameAttr = factory.getAttribute("system.target.files");
    
    if(systemFileNameAttr == null || systemFileNameAttr.length() == 0) {
      CloudataMetricsFactory.LOG.info("No system.target.files property in conf/cloudata-metrics.properties");
      return;
    }
    
    String[] fileNameTokens = systemFileNameAttr.split(",");
    for(String eachFileName: fileNameTokens) {
      String fileName = eachFileName.trim();
      File file = new File(fileName);
      if(!file.exists() || file.isDirectory()) {
        CloudataMetricsFactory.LOG.error(fileName + " not exists or not a file");
        continue;
      }
      String parserClass = factory.getAttribute("system." + fileName + ".parser");
      if(parserClass == null || parserClass.trim().length() == 0) {
        CloudataMetricsFactory.LOG.error("No [system." + fileName + ".parser] property in conf/cloudata-metrics.properties");
        continue;
      }
      try {
        SystemMetricsParser parser = (SystemMetricsParser)Class.forName(parserClass.trim()).newInstance();
        try {
          parser.init(fileName);
        } catch (IOException e) {
          CloudataMetricsFactory.LOG.error("Parser init error:" + fileName + ", " + parser + ":" + e.getMessage());
          continue;
        }
        systemMetircsFiles.put(fileName, parser);
        CloudataMetricsFactory.LOG.info("Add SystemMetrics[" + fileName + "," + parser + "]");
      } catch (Exception e) {
        CloudataMetricsFactory.LOG.error("Can't make parser instance:" + e.getMessage());
      }
      existsFile = true;
    }
  }
  
  public void doUpdates(CloudataMetricsContext context) {
    if(!existsFile) {
      return;
    }

    updateVmstat(context);
    
    for(Map.Entry<String, SystemMetricsParser> entry: systemMetircsFiles.entrySet()) {
      String fileName = entry.getKey();
      
      SystemMetricsParser parser = entry.getValue();

      String metricsPropertyName = metricsName + ".system." + fileName;
      context.clearMetricsData(metricsPropertyName);
      
      Map<String, Object> values = parser.getMetricsValues();
      if(values == null || values.isEmpty()) {
        CloudataMetricsFactory.LOG.warn("No metrics data: " + fileName);
        continue;
      }
      
      for(Map.Entry<String, Object> valueEntry: values.entrySet()) {
        context.addMetricsData(metricsPropertyName,  new MetricsValue(valueEntry.getKey(), valueEntry.getValue()));
      }
      
      context.addMetricsData(metricsPropertyName, new MetricsValueSeperator());
    }
  }
  
  public void shutdown() {
    context.stopMonitoring();
  }
  
  private void updateVmstat(CloudataMetricsContext context) {
    String vmstatName = metricsName + ".system.vmstat";
    context.clearMetricsData(vmstatName);
    
    Runtime rt = Runtime.getRuntime();
    Process process = null;
    long startTime = System.currentTimeMillis();
    BufferedInputStream in = null;
    try {
      process = rt.exec("vmstat");
      in = new BufferedInputStream(process.getInputStream());
      
      StringBuilder sb = new StringBuilder(1000);
      
      while(true) {
        if(in.available() > 0) {
          byte[] buf = new byte[4096];  //vmstat result smaller than 4096
          int length = in.read(buf);
          if(length > 0) {
            sb.append(new String(buf, 0, length));
          }
        } 
        if(System.currentTimeMillis() - startTime > 100) {
          break;
        }
        Thread.sleep(10);
      }
      if(sb.length() > 0) {
        context.addMetricsData(vmstatName, new MetricsValue("", sb.toString()));
        context.addMetricsData(vmstatName, new MetricsValueSeperator());
      }
    } catch (Exception e) {
    } finally {
      if(in != null) {
        try {
          in.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      if(process != null) {
        process.destroy();
      }
    }
  }
}
