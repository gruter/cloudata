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
package org.cloudata.util.matrix;

import java.util.HashMap;
import java.util.Map;

public class Pearson implements Correlation {
  public Map<String, Map<String, Double>> getSimilarity(Map<String, Map<String, Double>> datas) {
    return null;
  }
  
  public static void main(String[] args) {
    Map<String, Map<String, Double>> datas = getTestDatas();
  }
  
  public static Map<String, Map<String, Double>> getTestDatas() {
    Map<String, Map<String, Double>> datas = new HashMap<String, Map<String, Double>>();
    
    Map<String, Double> data1 = new HashMap<String, Double>();
    data1.put("L", 2.5);
    data1.put("Sn", 3.5);
    data1.put("J", 3.0);
    data1.put("Su", 3.5);
    data1.put("Y", 2.5);
    data1.put("T", 3.0);

    Map<String, Double> data2 = new HashMap<String, Double>();
    data2.put("L", 3.0);
    data2.put("Sn", 3.5);
    data2.put("J", 1.5);
    data2.put("Su", 5.0);
    data2.put("Y", 3.5);
    data2.put("T", 3.0);

    Map<String, Double> data3 = new HashMap<String, Double>();
    data3.put("L", 2.5);
    data3.put("Sn", 3.0);
    data3.put("Su", 3.5);
    data3.put("T", 4.0);

    Map<String, Double> data4 = new HashMap<String, Double>();
    data4.put("Sn", 3.5);
    data4.put("J", 3.0);
    data4.put("T", 4.5);
    data4.put("Su", 4.0);
    data4.put("Y", 2.5);

    Map<String, Double> data5 = new HashMap<String, Double>();
    data5.put("L", 3.0);
    data5.put("Sn", 4.0);
    data5.put("J", 2.0);
    data5.put("Su", 3.0);
    data5.put("T", 3.0);
    data5.put("Y", 2.0);

    Map<String, Double> data6 = new HashMap<String, Double>();
    data6.put("L", 3.0);
    data6.put("Sn", 4.0);
    data6.put("T", 3.0);
    data6.put("Su", 5.0);
    data6.put("Y", 3.5);

    Map<String, Double> data7 = new HashMap<String, Double>();
    data7.put("Sn", 4.5);
    data7.put("Y", 1.0);
    data7.put("Su", 4.0);

    datas.put("Li-Ro", data1);
    datas.put("Ge-Se", data2);
    datas.put("Mi-Ph", data3);
    datas.put("Cl-Pu", data4);
    datas.put("Mi-La", data5);
    datas.put("Ja-Ma", data6);
    datas.put("Toby", data7);
    
    return datas;
  }
}
