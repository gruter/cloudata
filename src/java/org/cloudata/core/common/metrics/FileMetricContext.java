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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.cloudata.core.common.util.StringUtils;


/**
 * @author jindolk
 *
 */
public class FileMetricContext extends CloudataMetricsContext {
  private static SimpleDateFormat df = new SimpleDateFormat("[yyyy-MM-dd HH:mm:ss]");
  private String fileName;
  
  private BufferedOutputStream out;
  
  private boolean printContextName = false;
  
  public synchronized void startMonitoring() {
    fileName = CloudataMetricsFactory.getFactory().getAttribute(contextName + ".log.fileName");
    
    fileName = fileName.replace("{0}", metricsName);
    
    try {
      File file = new File(fileName);
      if(file.getParent() != null && !file.getParentFile().exists()) {
        FileUtils.forceMkdir(file.getParentFile());
      }
      CloudataMetricsFactory.LOG.info("create metrics file:" + fileName);
      out = new BufferedOutputStream(new FileOutputStream(fileName, true));
    } catch (IOException e) {
      CloudataMetricsFactory.LOG.error("can't open output file: " + fileName, e);
      return;
    }

    String period = CloudataMetricsFactory.getFactory().getAttribute(contextName + ".period");
    if(period != null) {
      try {
        super.setPeriod(Integer.parseInt(period));
      } catch (NumberFormatException e) {
        CloudataMetricsFactory.LOG.warn("wrong " + contextName + ".period property [" +  period + "]");
      }
    }

    if("true".equals(CloudataMetricsFactory.getFactory().getAttribute(contextName + ".printContextName"))) {
      printContextName = true;
    }
    super.startMonitoring();
  }
  
  @Override
  protected void emitRecords(String name, List<MetricsValue> metricsValues) {
    Calendar cal = Calendar.getInstance();
    String logDate = df.format(cal.getTime());
    
    try {
      String keyLine;
      
      if(printContextName) {
        keyLine = logDate + "\t" + name;
      } else {
        keyLine = logDate;
      }
      
      String valueLine = StringUtils.leftPad("", keyLine.getBytes().length, ' ');
      
      boolean neededPrint = false;
      for(MetricsValue eachValue: metricsValues) {
        if(eachValue.isNewLine()) {
          out.write(keyLine.getBytes());
          out.write("\n".getBytes());
          out.write(valueLine.getBytes());
          out.write("\n".getBytes());
          
          neededPrint = false;
          if(printContextName) {
            keyLine = logDate + "\t" + name;
          } else {
            keyLine = logDate;
          }
          
          valueLine = StringUtils.leftPad("", keyLine.getBytes().length, ' ');          
        } else {
          if(eachValue.getKey().length() > 0) {
            keyLine += "\t" + eachValue.getKey();
            valueLine += "\t" + eachValue.getValueAsString();
          } else {
            valueLine = valueLine.trim() + eachValue.getValueAsString();
          }
          neededPrint = true;
        }
      }
      
      if(neededPrint) {
        out.write(keyLine.getBytes());
        out.write("\n".getBytes());
        out.write(valueLine.getBytes());
        out.write("\n".getBytes());
      }
    } catch (IOException e) {
      CloudataMetricsFactory.LOG.error("Can't FileMetricContext.emitRecords:" + e.getMessage());
    }
  }
  
  @Override
  protected boolean isMonitoring() {
    return true;
  }

  @Override
  protected void flush() {
    try {
      out.write("----------------------------------------------------------------------\n".getBytes());
      out.flush();
    } catch (IOException e) {
      CloudataMetricsFactory.LOG.error(e);
    }
  }
  
  public String toString() {
    return "FileMetricContext:" + metricsName + "," + contextName;
  }
}
