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

import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;

class ScanCellItem implements Comparable<ScanCellItem> {
  protected ScanCell scanCell;

  protected Integer scannerIndex;

  public ScanCellItem(ScanCell scanCell, int scannerIndex) {
    this.scanCell = scanCell;
    this.scannerIndex = scannerIndex;
  }

  public Row.Key getRowKey() {
    return scanCell.getRowKey();
  }

  public int compareTo(ScanCellItem item) {
    int compare = scanCell.getRowKey().compareTo(item.getRowKey());
    if (compare == 0) {
      return scannerIndex.compareTo(item.scannerIndex);
    } else
      return compare;
  }
  
  public boolean equals(Object obj) {
    if( !(obj instanceof ScanCellItem) ) {
      return false;
    }
    
    return compareTo((ScanCellItem)obj) == 0;
  }   
}
