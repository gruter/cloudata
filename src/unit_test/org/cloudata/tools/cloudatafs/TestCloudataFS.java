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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.cloudata.core.common.CloudataClusterTestCase;
import org.cloudata.tools.cloudatafs.CloudataFS;
import org.cloudata.tools.cloudatafs.FileNode;

import junit.framework.Test;
import junit.framework.TestSuite;


/**
 * @author jindolk
 *
 */
public class TestCloudataFS extends CloudataClusterTestCase {
  public TestCloudataFS(String name) {
    super(name);
  }

  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTest(new TestCloudataFS("testCloudataFS"));
    return suite;
  }
  
  public void testCloudataFS() throws Exception {
    CloudataFS fs = CloudataFS.get(conf);
    
    //0. check root
    assertTrue(fs.exists("/"));
    
    //1. mkdir
    fs.mkdir("/test01");
    fs.mkdir("/test02");
    fs.mkdir("/test01/sub01");
    fs.mkdirs("/test03/sub01/sub01-subA");
    
    //2. exists
    assertTrue(fs.exists("/test01"));
    assertTrue(fs.exists("/test02"));
    assertTrue(fs.exists("/test01/sub01"));
    assertFalse(fs.exists("/sub01"));
    assertTrue(fs.exists("/test03"));
    assertTrue(fs.exists("/test03/sub01"));
    assertTrue(fs.exists("/test03/sub01/sub01-subA"));
    
    //3. dir list
    fs.mkdir("/test03/sub02");
    fs.mkdir("/test03/sub03");
    fs.mkdir("/test03/sub01/sub01-subC");
    fs.mkdir("/test03/sub01/sub01-subB");
    fs.mkdir("/test01/sub01/sub01-subA");
    
    String[] paths = fs.listPath("/");
    assertNotNull(paths);
    Arrays.sort(paths);
    assertEquals(3, paths.length);
    assertEquals("/test01", paths[0]);
    assertEquals("/test02", paths[1]);
    assertEquals("/test03", paths[2]);
    
    paths = fs.listPath("/test03");
    assertNotNull(paths);
    Arrays.sort(paths);
    assertEquals(3, paths.length);
    assertEquals("/test03/sub01", paths[0]);
    assertEquals("/test03/sub02", paths[1]);
    assertEquals("/test03/sub03", paths[2]);

    paths = fs.listPath("/test03/sub01");
    assertNotNull(paths);
    Arrays.sort(paths);
    assertEquals(3, paths.length);
    assertEquals("/test03/sub01/sub01-subA", paths[0]);
    assertEquals("/test03/sub01/sub01-subB", paths[1]);
    assertEquals("/test03/sub01/sub01-subC", paths[2]);

    paths = fs.listPath("/test03/sub02");
    assertNull(paths);

    //4-0. create file in not exists dir or root dir
    try {
      fs.create("/");
      fail("not occur exception when create file with root dir");
    } catch (IOException e) {
    }

    try {
      fs.create("/emptydir/test01.dat");
      fail("not occur exception when create file in empty dir");
    } catch (IOException e) {
    }

    //4. create file
    String fileName = "/test03/sub01/test_file.dat";
    OutputStream out = fs.create(fileName);
    long writeLength = 0;
    DecimalFormat df = new DecimalFormat("0000000000");
    String data = "012345678900123456789001234567890012345678900123456789001234567890012345678900123456789001234567890";
    for(int i = 0; i < 100; i++) {
      byte[] writeData = (df.format(i) + data + "\n").getBytes();
      out.write(writeData);
      writeLength += writeData.length; 
    }
    out.close();
    
    assertTrue(fs.exists(fileName));
    assertFalse(fs.isDir(fileName));
    paths = fs.listPath("/test03/sub01");
    assertNotNull(paths);
    Arrays.sort(paths);
    assertEquals(4, paths.length);
    assertEquals("/test03/sub01/sub01-subA", paths[0]);
    assertEquals("/test03/sub01/sub01-subB", paths[1]);
    assertEquals("/test03/sub01/sub01-subC", paths[2]);
    assertEquals(fileName, paths[3]);
    
    //5. file length
    assertEquals(writeLength, fs.getLength(fileName));
    
    //6. open file
    InputStream in = fs.open(fileName);
    byte[] buf = new byte[(int)writeLength/100];
    int index = 0;
    int readLength = 0;
    int readSum = 0;
    while((readLength = in.read(buf)) > 0) {
      readSum += readLength;
      String readData = new String(buf, 0, readLength);
      assertEquals(df.format(index) + data + "\n", readData);
      index++;
    }
    in.close();
    
    assertEquals(writeLength, readSum);
    assertEquals(100, index);

    //7. open BufferedReader
    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(fileName)));
    String line = null;
    index = 0;
    while((line = reader.readLine()) != null) {
      assertEquals(df.format(index) + data, line);
      index++;
    }
    reader.close();
    
    assertEquals(100, index);
    
    //8.iterator
    String parentPath = "/iteratortest";
    fs.mkdir(parentPath);
    for(int i = 0; i < 450; i++) {
      fs.mkdir(parentPath + "/" + df.format(i));
    }
    Iterator<FileNode> nodes = fs.iterator(parentPath);
    assertNotNull(nodes);
    
    index = 0;
    while(nodes.hasNext()) {
      FileNode fileNode = nodes.next();
      assertNotNull(fileNode);
      assertEquals(parentPath + "/" + df.format(index), fileNode.getPath());
      assertTrue(fileNode.isDir());
      index++;
    }
    
    assertEquals(450, index);
    
    //9. move
    List<String> beforePathList = ls(fs, "/test03");
    assertTrue(fs.move("/test03", "/move/test03"));
    assertTrue(fs.exists("/move/test03"));
    assertFalse(fs.exists("/test03"));
    
    for(String beforePath: beforePathList) {
      assertFalse(fs.exists(beforePath));
      
      String checkPath = "/move/test03" + beforePath.substring("/test03".length());
      assertTrue(fs.exists(checkPath));
    }
    
    reader = new BufferedReader(new InputStreamReader(fs.open("/move/test03/sub01/test_file.dat")));
    line = null;
    index = 0;
    while((line = reader.readLine()) != null) {
      assertEquals(df.format(index) + data, line);
      index++;
    }
    reader.close();
    
    assertEquals(100, index);
    
    //10. delete
    try {
      fs.delete("/move/test03/sub01", false);
      fail("delete error");
    } catch(IOException e) {
    }
    assertTrue(fs.exists("/move/test03/sub01"));
    
    assertTrue(fs.delete("/move/test03/sub01/sub01-subC"));
    assertTrue(!fs.exists("/move/test03/sub01/sub01-subC"));
    
    assertTrue(fs.delete("/move/test03", true));
    assertTrue(!fs.exists("/move/test03"));
    assertTrue(!fs.exists("/move/test03/sub01/test_file.dat"));
  }
  
  private List<String> ls(CloudataFS fs, String path) throws IOException {
    List<String> fileList = new ArrayList<String>();
    
    FileNode[] fileNodes = fs.listFileNode(path);
    if(fileNodes == null || fileNodes.length == 0) {
      return fileList;
    }
    
    for(FileNode eachFileNode: fileNodes) {
      if(eachFileNode.isDir()) {
        fileList.addAll(ls(fs, eachFileNode.getPath()));
      } else {
        fileList.add(eachFileNode.getPath());
      }
    }
    
    return fileList;
  }
  
  public static void main(String[] args) throws Exception {
    junit.textui.TestRunner.run(TestCloudataFS.suite());
  }
}
