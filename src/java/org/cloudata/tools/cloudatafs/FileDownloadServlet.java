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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.cloudata.core.common.conf.CloudataConf;


/**
 * @author jindolk
 *
 */
public class FileDownloadServlet extends HttpServlet {
  private CloudataConf conf = new CloudataConf();
  private CloudataFS fs;
  
  @Override
  public void init() throws ServletException {
    try {
      fs = CloudataFS.get(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void service(HttpServletRequest request, HttpServletResponse response) throws ServletException {
    String path = request.getParameter("downloadPath");

    BufferedInputStream bis = null;
    BufferedOutputStream bos = null;

    try {
      FileNode fileNode = fs.getFileNode(path);
      
      if(fileNode == null) {
        response.getWriter().println("No file:" + path);
        return;
      }
      response.setHeader("Cache-Control","no-cache"); 
      response.setHeader("Content-Type","application/octet-stream"); 
      response.setHeader("Content-Disposition","attachment; filename=" + fileNode.getPathName()); 
  
      bis = new BufferedInputStream(fs.open(path));

      bos = new BufferedOutputStream(response.getOutputStream());
      
      int length = -1;
  
      byte[] buffer = new byte[4096];
      while((length = bis.read(buffer)) > 0) {
        bos.write(buffer, 0, length);
        bos.flush();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    finally {
      if(bis != null) {
        try {
          bis.close();
        } catch (IOException e) {
        }
      } 
      if(bos != null) {
        try {
          bos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
