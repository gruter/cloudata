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
package org.cloudata.core.client.blob;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


/**
 * @author jindolk
 * USER_TABLE.COLUMN: CK=cellKey, VALUE=path + offset + length
 * BLOB.RK: path
 * BLOB.OFFSET: CK=offset, VALUE=usertable.name+rowKey+column+cellKey
 * BLOB.LOCK: CK=lock count, VALUE=null
 * BLOB.DELETED: CK=offset, VALUE=Y/N
 */
public class CBlobOutputStream extends OutputStream implements Constants {
  private static final Log LOG = LogFactory.getLog(CBlobOutputStream.class.getName());
  
  //static final DecimalFormat SEQ_DF = new DecimalFormat("00000");
  
  public static final DecimalFormat OFFSET_DF = new DecimalFormat("00000000000000000000");
  
  private CloudataConf conf;
  private BlobMetaCell blobMetaCell;
  
  private String tempFile;
  private String fileName;
  private OutputStream out;
  private CloudataFileSystem fs;
  private long length;
  
  public CBlobOutputStream(CloudataConf conf, BlobMetaCell blobMetaCell) throws IOException {
    this.conf = conf;
    this.blobMetaCell = blobMetaCell;
    
    this.fs = CloudataFileSystem.get(conf);

    Random rand = new Random(System.currentTimeMillis());
    
    this.fileName = System.currentTimeMillis() + System.nanoTime() + blobMetaCell.toString().hashCode() + "" + rand.nextInt();

    GPath parentPath = new GPath(BlobMetaManager.getBlobTempPath(conf, blobMetaCell.getTableName(), blobMetaCell.getColumnName()));
    fs.mkdirs(parentPath);
    
    tempFile = parentPath + "/" + fileName;
    
    //LOG.info("Output Open:" + tempFile);
    this.out = fs.create(new GPath(tempFile));
  }

  @Override
  public void write(int b) throws IOException {
    out.write(b);
  }

  @Override
  public void close() throws IOException {
    //LOG.info("Output close:" + tempFile);

    out.close();
    
    GPath parentPath = new GPath(BlobMetaManager.getBlobRootPath(conf, blobMetaCell.getTableName(), blobMetaCell.getColumnName()));
    fs.mkdirs(parentPath);
    
    GPath targetPath = new GPath(parentPath, fileName);
    
    fs.move(new GPath(tempFile), targetPath);
    
    length = fs.getLength(targetPath);
    
    //System.out.println("length>>>>>>>>>>>>>" + targetPath + ">" + length);
    blobMetaCell.setLength(length);
    
    BlobColumnCell blobColumnCell = new BlobColumnCell(targetPath.toString(), 0L, length);
    
    BlobMetaManager.saveBlobMetaCell(conf, blobMetaCell, blobColumnCell);
    BlobMetaManager.saveBlobColumnCell(conf, blobMetaCell, blobColumnCell);
  }
  
  @Override
  public void flush() throws IOException {
    out.flush();
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    out.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    out.write(b);
  }
  
  public long getLength() {
    return length;
  }
}
