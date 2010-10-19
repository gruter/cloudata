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
package org.cloudata.core.client;

import java.io.IOException;

/**
 * NTable has a get() method which allows applications to asynchronously receive data.
 * The result of retrieval is returned through DataReceiver specified in the parameter.
 */
public interface AsyncDataReceiver {

  /**
   * Called when single Row fetched. 
   * @param rowKey
   * @param row
   * @throws IOException
   */
  public void receive(Row.Key rowKey, Row row) throws IOException;
  
  /**
   * Called when received exception
   * @param rowKey
   * @param exception
   */
  public void error(Row.Key rowKey, Exception exception);
  
  /**
   * Called when timed out
   */
  public void timeout();
}
