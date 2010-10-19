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
package org.cloudata.core.client.browser;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.util.Vector;

import javax.swing.JOptionPane;

import org.cloudata.core.common.util.StringUtils;


public class QueryPanelModel {
  private Vector<String> queryBuffer = new Vector<String>();
  
  public QueryPanelModel() {
    loadQuery();
  }
  
  public String getQuery(int index) {
    return queryBuffer.get(index);
  }
  
  public int getQuerySize() {
    return queryBuffer.size();
  }

  public void addQuery(String query) {
    queryBuffer.add(query);
  }
  
  private void loadQuery() {
    BufferedReader reader = null;
    try {
      File file = new File(BrowserConstants.QUERY_FILE);
      if(!file.exists()) {
        return;
      }
      reader = new BufferedReader(new InputStreamReader(new FileInputStream(BrowserConstants.QUERY_FILE)));
      
      StringBuffer sb = new StringBuffer();
      String line = null;
      
      while( (line = reader.readLine()) != null) {
        if(line.trim().length() == 0) {
          continue;
        }
        if(BrowserConstants.QUERY_DELIM.equals(line)) {
          queryBuffer.add(sb.toString());
          sb.setLength(0);
        } else {
          sb.append(line).append("\n");
        }
      }
    } catch (Exception e) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, StringUtils.stringifyException(e), "Error",
          JOptionPane.ERROR_MESSAGE);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception err) {
        }
      }
    }
  }
  
  public synchronized void saveQuery(String query) {
    BufferedOutputStream out = null;
    try {
      out = new BufferedOutputStream(new FileOutputStream(BrowserConstants.QUERY_FILE, true));
      out.write("\n".getBytes());
      out.write(query.getBytes());
      out.write("\n".getBytes());
      out.write(BrowserConstants.QUERY_DELIM.getBytes());
    } catch (Exception e) {
      JOptionPane.showMessageDialog(CloudataBrowser.frame, StringUtils.stringifyException(e), "Error",
          JOptionPane.ERROR_MESSAGE);
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (Exception err) {
        }
      }
    }
  }

  public void clear() {
    queryBuffer.clear();
    queryBuffer = null;
  }  
}
