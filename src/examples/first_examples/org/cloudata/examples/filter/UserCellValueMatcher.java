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
package org.cloudata.examples.filter;

import org.cloudata.core.client.CellValueMatcher;

/**
 * Notice!!!
 * User defined CellValueMatcher class can't be Inner class and
 * can uses classes in Cloudata's class path 
 * @author jindolk
 *
 */
public class UserCellValueMatcher implements CellValueMatcher {
  public static int VERSION = 1;

  @Override
  public boolean match(byte[] value, long timestamp, String[] filterParams) {
    InnerValue strValue = new InnerValue(value);
    if(strValue.getValue().indexOf(filterParams[0]) >= 0) {
      return true;
    }
    return false;
  }

  static class InnerValue {
    String valueStr;
    public InnerValue(byte[] data) {
      if(data != null) {
        valueStr = new String(data);
      } else {
        valueStr = "";
      }
    }
    
    public String getValue() {
      return valueStr;
    }
  }

}
