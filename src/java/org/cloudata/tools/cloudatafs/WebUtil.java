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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.commons.fileupload.FileItem;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class WebUtil {
  static class FileNodeComparator implements Comparator<FileNode> {
    String sortField;
    boolean asc;
    public FileNodeComparator(String sortField, boolean asc) {
      this.sortField = sortField;
      this.asc = asc;
    }
    
    @Override
    public int compare(FileNode o1, FileNode o2) {
      FileNode f1 = o1;
      FileNode f2 = o2;
      if(!asc) {
        f1 = o2;
        f2 = o1;
      }
      
      if("name".equals(sortField)) {
        return f1.getPath().compareTo(f2.getPath());
      } else if("date".equals(sortField)) {
        return (new Long(f1.getDate())).compareTo(new Long(f2.getDate()));
      } else if("size".equals(sortField)) {
        return (new Long(f1.getLength())).compareTo(new Long(f2.getLength()));
      } else if("type".equals(sortField)) {
        return (new Integer(f1.getType())).compareTo(new Integer(f2.getType()));
//        if(f1.getType() == f2.getType()) {
//          return f1.getPath().compareTo(f2.getPath());
//        } else {
//          
//        }
      } else {
        return f1.getPath().compareTo(f2.getPath());
      }
    }
  }
  
  public static String listDir(CloudataFS fs, String path, String sortField, String orderBy) throws IOException {
    if(sortField == null || sortField.length() == 0) {
      sortField = "name";
    }
    
    if(orderBy == null || orderBy.length() == 0) {
      orderBy = "asc";
    }
    
    FileNode[] fileNodes = fs.listFileNode(path);

    if(fileNodes == null) {
      fileNodes = new FileNode[]{};
    }
    StringBuilder sb = new StringBuilder();
    
    sb.append("{ \"page\":\"1\",");
    sb.append("\"total\":\"" + fileNodes.length +"\",");
    sb.append("\"records\":\"" + fileNodes.length +"\",");
    sb.append("\"rows\":[");

    Arrays.sort(fileNodes, new FileNodeComparator(sortField, "asc".equals(orderBy)));
    
//    sb.append("{");
//    sb.append((new ParentFileNode(CloudataFS.getParentPath(path))).toJsonString());
//    sb.append("}");
    int index = 0;
    
    for (FileNode fileNode: fileNodes) {
      sb.append("{");
      sb.append(fileNode.toJsonString("file_" + index));
      sb.append("}");
      if(index < fileNodes.length - 1) {
        sb.append(",");
      }
      index++;
    } 
    sb.append("]}");
     
    //System.out.println(sb.toString());
    
    return sb.toString();
  }

  public static String getImage(FileNode fileNode) {
    String imagePath = "lib/jqueryFileTree/";
    
    if(fileNode.getType() == FileNode.DIR) {
      return imagePath + "images/directory.png";
    }
    String ext = fileNode.getExt();
    if(ext == null) {
      return imagePath + "images/file.png";
    }
    if(ext.equals("..")) { return null; }
    
    if(ext.equals("3gp"))   { return imagePath + "images/film.png"; }
    if(ext.equals("afp"))   { return imagePath + "images/code.png"; }
    if(ext.equals("afpa"))  { return imagePath + "images/code.png"; }
    if(ext.equals("asp"))   { return imagePath + "images/code.png"; }
    if(ext.equals("aspx"))  { return imagePath + "images/code.png"; }
    if(ext.equals("avi"))   { return imagePath + "images/film.png"; }
    if(ext.equals("bat"))   { return imagePath + "images/application.png"; }
    if(ext.equals("bmp"))   { return imagePath + "images/picture.png"; }
    if(ext.equals("c"))     { return imagePath + "images/code.png"; }
    if(ext.equals("cfm"))   { return imagePath + "images/code.png"; }
    if(ext.equals("cgi"))   { return imagePath + "images/code.png"; }
    if(ext.equals("com"))   { return imagePath + "images/application.png"; }
    if(ext.equals("cpp"))   { return imagePath + "images/code.png"; }
    if(ext.equals("css"))   { return imagePath + "images/css.png"; }
    if(ext.equals("doc"))   { return imagePath + "images/doc.png"; }
    if(ext.equals("docx"))   { return imagePath + "images/doc.png"; }
    if(ext.equals("exe"))   { return imagePath + "images/application.png"; }
    if(ext.equals("gif"))   { return imagePath + "images/picture.png"; }
    if(ext.equals("fla"))   { return imagePath + "images/flash.png"; }
    if(ext.equals("h"))     { return imagePath + "images/code.png"; }
    if(ext.equals("htm"))   { return imagePath + "images/html.png"; }
    if(ext.equals("html"))  { return imagePath + "images/html.png"; }
    if(ext.equals("jar"))   { return imagePath + "images/java.png"; }
    if(ext.equals("jpg"))   { return imagePath + "images/picture.png"; }
    if(ext.equals("jpeg"))  { return imagePath + "images/picture.png"; }
    if(ext.equals("js"))    { return imagePath + "images/script.png"; }
    if(ext.equals("lasso")) { return imagePath + "images/code.png"; }
    if(ext.equals("log"))   { return imagePath + "images/txt.png"; }
    if(ext.equals("m4p"))   { return imagePath + "images/music.png"; }
    if(ext.equals("mov"))   { return imagePath + "images/film.png"; }
    if(ext.equals("mp3"))   { return imagePath + "images/music.png"; }
    if(ext.equals("mp4"))   { return imagePath + "images/film.png"; }
    if(ext.equals("mpg"))   { return imagePath + "images/film.png"; }
    if(ext.equals("mpeg"))  { return imagePath + "images/film.png"; }
    if(ext.equals("ogg"))   { return imagePath + "images/music.png"; }
    if(ext.equals("pcx"))   { return imagePath + "images/picture.png"; }
    if(ext.equals("pdf"))   { return imagePath + "images/pdf.png"; }
    if(ext.equals("php"))   { return imagePath + "images/php.png"; }
    if(ext.equals("png"))   { return imagePath + "images/picture.png"; }
    if(ext.equals("ppt"))   { return imagePath + "images/ppt.png"; }
    if(ext.equals("pptx"))   { return imagePath + "images/ppt.png"; }
    if(ext.equals("psd"))   { return imagePath + "images/psd.png"; }
    if(ext.equals("pl"))    { return imagePath + "images/script.png"; }
    if(ext.equals("py"))    { return imagePath + "images/script.png"; }
    if(ext.equals("rb"))    { return imagePath + "images/ruby.png"; }
    if(ext.equals("rbx"))   { return imagePath + "images/ruby.png"; }
    if(ext.equals("rhtml")) { return imagePath + "images/ruby.png"; }
    if(ext.equals("rpm"))   { return imagePath + "images/linux.png"; }
    if(ext.equals("ruby"))  { return imagePath + "images/ruby.png"; }
    if(ext.equals("sql"))   { return imagePath + "images/db.png"; }
    if(ext.equals("swf"))   { return imagePath + "images/flash.png"; }
    if(ext.equals("tif"))   { return imagePath + "images/picture.png"; }
    if(ext.equals("tiff"))  { return imagePath + "images/picture.png"; }
    if(ext.equals("txt"))   { return imagePath + "images/txt.png"; }
    if(ext.equals("vb"))    { return imagePath + "images/code.png"; }
    if(ext.equals("wav"))   { return imagePath + "images/music.png"; }
    if(ext.equals("wmv"))   { return imagePath + "images/film.png"; }
    if(ext.equals("xls"))   { return imagePath + "images/xls.png"; }
    if(ext.equals("xlsx"))   { return imagePath + "images/xls.png"; }
    if(ext.equals("xml"))   { return imagePath + "images/code.png"; }
    if(ext.equals("zip"))   { return imagePath + "images/zip.png"; }
    
    return imagePath + "images/file.png";
  }
  
  public static void saveFile(CloudataConf conf, FileItem fileItem, String parentPath) throws IOException {
    BufferedInputStream uploadedStream = new BufferedInputStream(fileItem.getInputStream());
    CloudataFS fs = CloudataFS.get(conf);
    //BufferedOutputStream serverFileOut = new BufferedOutputStream(new FileOutputStream("c:/temp/bb/" + fileItem.getName()));
    BufferedOutputStream serverFileOut = new BufferedOutputStream(fs.create(parentPath + fileItem.getName()));
    
    try {
      byte[] buf = new byte[4096];
      int readLen = 0;
      while( (readLen = uploadedStream.read(buf)) > 0) {
        serverFileOut.write(buf, 0, readLen);
      }
    } finally {
      serverFileOut.close();
      uploadedStream.close();
    }
    
    fileItem.delete();
  }  
}
