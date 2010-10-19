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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;


abstract class AbstractTableScanner implements TableScanner {
  private static final Log LOG = LogFactory.getLog(AbstractTableScanner.class.getName());
  
  private ScanCell lastCell;
  
  protected String columnName;
  
  protected abstract boolean isEnd();

  protected abstract void setEnd(boolean end);

  protected void setLastCell(ScanCell lastCell) {
    this.lastCell = lastCell;
  }
  
  @Override
  public Row nextRow() throws IOException {
    try {
      if(isEnd()) {
        return null;
      }
      
      List<ScanCell> result = new ArrayList<ScanCell>();
      while(true) {
        ScanCell cell = next();
        if(cell == null) {
          if(lastCell != null) {
            result.add(lastCell);
            lastCell = cell;
          }
          setEnd(true);
          break;
        }
        
        if(lastCell != null && !cell.getRowKey().equals(lastCell.getRowKey())) {
          result.add(lastCell);
          lastCell = cell;
          break;
        }
  
        if(lastCell != null) {
          result.add(lastCell);
        }
        
        lastCell = cell;
      }
      
      if(result.size() == 0) {
        setEnd(true);
        return null;
      }
      
      Row row = new Row(result.get(0).getRowKey());
      
      ScanCell.addCells(row, columnName, result);
      
      return row;
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      IOException err = new IOException(e.getMessage());
      err.initCause(e);
      throw err;
    }
  } 
}
