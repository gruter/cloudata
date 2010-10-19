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

import java.util.List;

/**
 * @author jindolk
 *
 */
public class DummyMetricContextWithUpdate extends CloudataMetricsContext {

  @Override
  protected void emitRecords(String name, List<MetricsValue> metricsValues) {
//    for(MetricsValue eachMetricsValue: metricsValues) {
//      System.out.println(eachMetricsValue.getKey() + ":" + eachMetricsValue.getValueAsString());
//    }    
  }


  protected boolean isMonitoring() {
    return true;
  }

  @Override
  protected void flush() {
  }
  
  public String toString() {
    return "DummyMetricContextWithUpdate:" + metricsName + "," + contextName;
  }
}
