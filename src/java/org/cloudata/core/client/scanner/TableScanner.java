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
package org.cloudata.core.client.scanner;

import java.io.IOException;

import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;


/**
 * Interface of Client-side scanner. Can get TableScanner object with ScannerFactory class.<BR>
 * When TableScanner initiated, TableScanner runs background thread which sends a signal to touch scanner's timeout to TabletServer<BR>
 * So if you don't call close method explicitly, Your program never terminated.<BR>
 * TableScanner must be used for batch processing like mapreduce job. And if TableScanner is opened, Split and Compaction operation are blocked. 
 *
 */
public interface TableScanner {
  public static final int SCANNER_OPEN_TIMEOUT = 120;   //sec
  
  /**
   * Fetch next data<BR>
   * Can't use next, nextRow simultaneous.
   * @return If no more data, return null 
   * @throws IOException
   */
  public ScanCell next() throws IOException;

  /**
   * Fetch next row.<BR>
   * Can't use next, nextRow simultaneous.
   * @return If no more data, return null 
   * @throws IOException
   */
  public Row nextRow() throws IOException;

  /**
   * Close scanner. You must be close after open scanner like next<BR>
   * <pre>
   * TableScanner scanner =  null; 
   * 
   * try {
   *   scanner = ScannerFactory.openScanner(...);
   * } finally {
   *   if(scanner != null) {
   *     scanner.close();
   *   }
   * }
   * </pre>
   * @throws IOException
   */
  public void close() throws IOException ;

  /**
   * Return scanner's column list
   * @return
   */
  public String[] getColumnNames();
}
