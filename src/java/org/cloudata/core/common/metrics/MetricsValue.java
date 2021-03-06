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

import java.text.DecimalFormat;

/**
 * @author jindolk
 *
 */
public class MetricsValue {
  private static DecimalFormat df = new DecimalFormat("0.000");
  
  private String key;
  private Object value;
  
  public MetricsValue(String key, Object value) {
    this.key = key;
    this.value = value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
  
  public Object getValue() {
    return value;
  }
  
  public String getKey() {
    return key;
  }
  
  public String toString() {
    return key + ":" + value;
  }

  public String getValueAsString() {
    if(value instanceof Number) {
      return df.format(((Number)value).doubleValue());
    } else {
      return value.toString();
    }
  }
  
  public boolean isNewLine() {
    return false;
  }
  
  public static class MetricsValueSeperator extends MetricsValue {
    public MetricsValueSeperator() {
      super("", "");
    }
    public boolean isNewLine() {
      return true;
    }
  }
}
