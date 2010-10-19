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
package org.cloudata.tools.cloudatafs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.Row;


/**
 * @author jindolk
 *
 */
public class FileNodeIterator implements Iterator<FileNode> {
  public static final Log LOG = LogFactory.getLog(FileNodeIterator.class.getName());
  
  private Iterator<Row> iterator;
  
  public FileNodeIterator(Iterator<Row> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public FileNode next() {
    Row row = iterator.next();
    
    Cell cell = row.getFirst(CloudataFS.INODE_COLUMN);
    if(cell == null) {
      LOG.error(CloudataFS.INODE_COLUMN + " is null");
      return null;        
    }
    
    FileNode fileNode;
    try {
      fileNode = new FileNode(cell.getBytes());
    } catch (IOException e) {
      LOG.error(e);
      return null;
    }
    return fileNode;
  }

  @Override
  public void remove() {
    
  }

}
