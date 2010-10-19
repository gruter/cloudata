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
package org.cloudata.core.common.aop;

import java.io.IOException;

public class ExitTestProcessor implements DistributedUnitTestProcessor {
  public boolean runTest(TestHandler testHandler, UnitTestDetail unitTestDetail) throws Exception {
    System.out.println("========================================");
    System.out.println("System.exit() for unit test");
    System.out.println("========================================");
    System.exit(0);
    return false;
  }
  public boolean isTestTarget(TestHandler testHandler) throws IOException {
    return true;
  }
}
