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
package org.cloudata.examples.upload;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class UploadUtil {
  public static void generateData(String path) throws IOException {
    DecimalFormat df = new DecimalFormat("0000000000000");
    String title = "this is test title.";
    for(int i = 0; i < 1000; i++) {
      title += (i % 10);
    }
    
    FileSystem fs = FileSystem.get(new Configuration());
    
    if(fs.exists(new Path(path, "data01"))) {
      return;
    }
    OutputStream out = fs.create(new Path(path, "data01"));
    
    Random rand = new Random();
    for(int i = 0; i < 1000000; i++) {
      String data = "rowkey" + df.format(rand.nextInt(10000000)) + "\t" + title + "\n";
      out.write(data.getBytes());
    }
    
    out.close();
  }
}
