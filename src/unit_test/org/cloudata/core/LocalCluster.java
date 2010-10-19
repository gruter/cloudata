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
package org.cloudata.core;

import org.cloudata.core.common.MiniCloudataCluster;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * Sinlge process에 클러스터에 참여하는 모든 데몬을 실행시켜 개발용이나 로컬에서 데이터 모델에 대한 검증 등
 * 간단한 테스트를 수행하는데 활용
 * @author jindolk
 *
 */
public class LocalCluster {
  public void startCluster(String dataDir) throws Exception {
    CloudataConf nconf = new CloudataConf();
    
    //nconf.setBoolean("testmode", true);
    new MiniCloudataCluster(nconf, 1, 3, true);
    System.out.println("================== Complete starting LocalCluster ======================");
  }
  
  public static void main(String[] args) throws Exception {
    String dataDir = null;
    if(args.length > 0) {
      dataDir = args[0];
    }
    
    (new LocalCluster()).startCluster(dataDir);
  }
}
