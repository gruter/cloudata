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
package org.cloudata.core.common.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.cloudata.core.fs.CloudataFileSystem;
import org.cloudata.core.fs.GPath;


public class FileUtil {
  public static void saveFile(File file, String savePath) {
    FileInputStream inputStream = null;
    ByteArrayOutputStream baos = null;
    try {
      inputStream = new FileInputStream(file);

      baos = new ByteArrayOutputStream();

      OutputStream bos = new FileOutputStream(savePath);
      int bytesRead = 0;
      byte[] buffer = new byte[8192];
      while ((bytesRead = inputStream.read(buffer, 0, 8192)) != -1) {
        bos.write(buffer, 0, bytesRead);
      }
    } catch (Exception e) {
    } finally {
      if (inputStream != null)
        try {
          inputStream.close();
        } catch (Exception e) {
        }
      if (baos != null)
        try {
          baos.close();
        } catch (Exception e) {
        }
    }
  }

  public static void saveFile(byte[] file, String savePath) {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(file, 0, file.length);
      baos.close();
    } catch (Exception e) {
    }
  }

  public static boolean moveFile(String oldFile, String newFile) {
    if (copyFile(oldFile, newFile)) {
      File file = new File(oldFile);
      //Windows file system 
      try {
        Thread.sleep(2 * 1000);
      } catch (InterruptedException e) {
      }
      file.delete();
      return true;
    } else {
      return false;
    }
  }

  public static boolean copyFile(String r_file, String w_file) {
    FileOutputStream foStream = null;
    String saveDirectory = null;
    File dir = null;

    try {
      int iIndex = w_file.lastIndexOf("/");

      if (iIndex > 0) {
        saveDirectory = w_file.substring(0, (w_file.lastIndexOf("/") + 1));

        dir = new File(replaceStr(saveDirectory, "//", "/"));

        if (!dir.isDirectory()) {
          dir.mkdirs();
        }

      }

      foStream = new FileOutputStream(w_file);
      return dumpFile(r_file, foStream);
    } catch (Exception ex) {
      CloudataFileSystem.LOG.error(ex);
      return false;
    } finally {
      try {
        if (foStream != null)
          foStream.close();
      } catch (Exception ex2) {
      }
    }
  }

  public static boolean dumpFile(String r_file, OutputStream outputstream) {
    byte abyte0[] = new byte[4096];
    boolean flag = true;
    FileInputStream fiStream = null;
    try {
      fiStream = new FileInputStream(r_file);
      int i;
      while ((i = fiStream.read(abyte0)) != -1)
        outputstream.write(abyte0, 0, i);

      fiStream.close();
    } catch (Exception ex) {
      try {
        if (fiStream != null)
          fiStream.close();
      } catch (Exception ex1) {
      }
      flag = false;
    } finally {
      try {
        if (fiStream != null)
          fiStream.close();
      } catch (Exception ex2) {
      }
    }

    return flag;
  }

  /**
   * 문자열에서 특정 문자열을 치환한다. 문자열 배열의 차례대로 치환하되 더 이상 배열 값이 없으면 space 1칸으로 치환한다.
   * 
   * @return the translated string.
   * @param source
   *          String 변환할 문자열
   * @param keyStr
   *          String 치환 대상 문자열
   * @param toStr
   *          String[] 치환될 문자열 배열
   */
  public static String replaceStr(String source, String keyStr, String[] toStr) {
    if (source == null)
      return null;
    int startIndex = 0;
    int curIndex = 0;
    int i = 0;
    StringBuffer result = new StringBuffer();
    String specialString = null;

    while ((curIndex = source.indexOf(keyStr, startIndex)) >= 0) {
      if (i < toStr.length)
        specialString = toStr[i++];
      else
        specialString = " ";
      result.append(source.substring(startIndex, curIndex)).append(specialString);
      startIndex = curIndex + keyStr.length();
    }

    if (startIndex <= source.length())
      result.append(source.substring(startIndex, source.length()));

    return result.toString();
  }

  public static String replaceStr(String source, String keyStr, String toStr) {
    if (source == null)
      return null;
    int startIndex = 0;
    int curIndex = 0;
    StringBuffer result = new StringBuffer();

    while ((curIndex = source.indexOf(keyStr, startIndex)) >= 0) {
      result.append(source.substring(startIndex, curIndex)).append(toStr);
      startIndex = curIndex + keyStr.length();
    }

    if (startIndex <= source.length())
      result.append(source.substring(startIndex, source.length()));

    return result.toString();

  }

  public static boolean delete(FileSystem fs, Path path, boolean recursive) throws IOException {
    if(!fs.exists(path)) {
      return true;
    }
    if(fs.getFileStatus(path).isDir()) {
      FileStatus[] srcs = fs.listStatus(path);
      int length = (srcs != null ? srcs.length : 0) ;
      for (int i = 0; i < length; i++) {
        if(recursive) {
          if(fs.getFileStatus(srcs[i].getPath()).isDir()) {
            if(!delete(fs, srcs[i].getPath(), recursive)) {
              return false;       
            }
          } else {
            if(!fs.delete(srcs[i].getPath())) {
              return false;
            }
          }
          //deleteAll(srcs[i], recursive);
        } else {
          if(!fs.delete(srcs[i].getPath())) {
            return false;
          }
        }
      }
    }
    if(!fs.delete(path)) {
      return false;  
    }
    
    return true;
  }
  
  public static boolean delete(String path, boolean recursive) throws IOException {
    File file = new File(path);
    if(file.isDirectory()) {
      File[] srcs = file.listFiles();
      int length = (srcs != null ? srcs.length : 0) ;
      for (int i = 0; i < length; i++) {
        if(recursive) {
          if(srcs[i].isDirectory()) {
            if(!delete(srcs[i].getAbsolutePath(), recursive)) {
              return false;       
            }
          } else {
            if(!srcs[i].delete()) {
              return false;
            }
          }
        } else {
          if(!srcs[i].delete()) {
            return false;
          }
        }
      }
    }
    if(!file.delete()) {
      return false;  
    }
    
    return true;
  }  
  
  public static long mergeSort(CloudataFileSystem fs, String[] inputPaths, String outputPath, InputParser parser) throws IOException {
    int fileIndex = 0;
    String tmpFileName = "_temp_merge_";
    List<BufferedReader> readerList = new ArrayList<BufferedReader>();
    
    for(String eachPath: inputPaths) {
      String fileName = tmpFileName + fileIndex;
      BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new GPath(eachPath))));
      
      String line = null;
      SortedSet<String> buf= new TreeSet<String>();
      while( (line = reader.readLine()) != null ) {
        buf.add(parser.parse(line));
      }
      
      BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(fileName));
      try {
        for(String eachRecord: buf) {
          out.write((eachRecord + "\n").getBytes());
        }
      } finally {
        out.close();
      }
      readerList.add(new BufferedReader(new InputStreamReader(new FileInputStream(fileName))));
      fileIndex++;
    }

    BufferedReader[] readers = readerList.toArray(new BufferedReader[]{});

    BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(outputPath));
    
    SortedSet<MergeSortRecord> buf = new TreeSet<MergeSortRecord>();
    long count = 0;
    try {
      while(true) {
        if(buf.size() == 0) {
          for(int i = 0; i < readers.length; i++) {
            if(readers[i] != null) {
              String line = readers[i].readLine();
              if(line == null) {
                readers[i].close();
                readers[i] = null;
              } else {
                buf.add(new MergeSortRecord(i, line));
              }
            }
          }
        }
        
        if(buf.size() == 0) {
          break;
        }
        
        MergeSortRecord winner = buf.first();
        buf.remove(winner);
        
        out.write((winner.str + "\n").getBytes());
        String line = readers[winner.index].readLine();
        if(line == null) {
          readers[winner.index].close();
          readers[winner.index] = null;
        } else {
          buf.add(new MergeSortRecord(winner.index, line));
        }
        count++;
      }
    } finally {
      if(out != null) {
        out.close();
      }
    }
    
    return count;
  }
  
  public static interface InputParser {
    public String parse(String line);
  }
  
  static class MergeSortRecord implements Comparable<MergeSortRecord> {
    int index;
    String str;
    
    public MergeSortRecord(int index, String str) {
      this.index = index;
      this.str = str;
    }

    @Override
    public int compareTo(MergeSortRecord o) {
      return str.compareTo(o.str);
    }
  }
}
