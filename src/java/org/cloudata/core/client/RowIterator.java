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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jindolk
 *
 */
public class RowIterator implements Iterator<Row> {
  private static final Log LOG = LogFactory.getLog(RowIterator.class.getName());
  private CTable ctable;
  private RowFilter rowFilter;
  private Row[] rows;
  private int index;
  private Row.Key previousRowKey; 
  private int pageSize;
  
  public RowIterator(CTable ctable, RowFilter rowFilter) throws IOException {
    this.ctable = ctable;
    this.rowFilter = rowFilter;
    pageSize = rowFilter.getPageSize();
    rows = ctable.gets(rowFilter);

    if(rows != null && rows.length > 0) {
      previousRowKey = rows[rows.length - 1].getKey();
    }
    index = -1;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#hasNext()
   */
  @Override
  public boolean hasNext() {
    if(rows != null && index >= rows.length - 1) {
      try {
        rowFilter.setPagingInfo(pageSize, previousRowKey);
        rows = ctable.gets(rowFilter);
        if(rows != null && rows.length > 0) {
          previousRowKey = rows[rows.length - 1].getKey();
        }
      } catch (IOException e) {
        LOG.error("Can't get next rows:" + e.getMessage(), e);
      }
      index = -1;
    }
    return rows != null;
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#next()
   */
  @Override
  public Row next() {
    if(rows != null && index >= rows.length - 1) {
      try {
        rowFilter.setPagingInfo(pageSize, previousRowKey);
        rows = ctable.gets(rowFilter);
        if(rows != null && rows.length > 0) {
          previousRowKey = rows[rows.length - 1].getKey();
        } else {
          return null;
        }
      } catch (IOException e) {
        LOG.error("Can't get next rows:" + e.getMessage(), e);
      }
      index = -1;
    }
    index++;
    
    return rows[index];
  }

  /* (non-Javadoc)
   * @see java.util.Iterator#remove()
   */
  @Override
  public void remove() {
  }
}
