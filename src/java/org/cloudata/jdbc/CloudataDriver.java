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
package org.cloudata.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.cloudata.core.client.CTableManager;
import org.cloudata.core.common.Constants;
import org.cloudata.core.common.conf.CloudataConf;


/**
 * <PRE>
 * String url = "jdbc:cloudata://pleiades_hostname:pleiades_port:clusterId/cloudata_service_name?user=test&charset=EUC-KR";
 * String url = "jdbc:cloudata://host01:8000:host01_cloudata/cloudata?user=test&charset=EUC-KR";
 * 
 * @author jindolk
 *
 */
public class CloudataDriver implements Driver {
  static final String CLOUDATA_PROTOCOL = "jdbc:cloudata://";
  
  static {
    try {
      DriverManager.registerDriver(new CloudataDriver());
    } catch( SQLException e) {
      e.printStackTrace(System.out);
    }
  }
  
  /* (non-Javadoc)
   * @see java.sql.Driver#acceptsURL(java.lang.String)
   */
  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(CLOUDATA_PROTOCOL);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if(!acceptsURL(url)) {
      return null;
    }
    String urlInfo = url.substring(CLOUDATA_PROTOCOL.length());
    
    int index = urlInfo.indexOf("/");
    String pleiadesHostInfo = urlInfo.substring(0, index);
    
    urlInfo = urlInfo.substring(index + 1);
    
    index = pleiadesHostInfo.indexOf(":");
    if(index <= 0) {
      throw new SQLException("Wrong url [" + url + "]");
    }
    CloudataConf conf = new CloudataConf();
    conf.set("cloudata.jdbc", "y");
    
    String hostName = pleiadesHostInfo.substring(0, index);
    String portStr = pleiadesHostInfo.substring(index + 1);
    index = portStr.indexOf(":");
    if(index <= 0) {
      throw new SQLException("Wrong url [" + url + "]");
    }
    String port = portStr.substring(0, index);
    String clusterId = portStr.substring(index + 1);
    
    String userId = null;
    String charset = null;
    
    index = urlInfo.indexOf("?");
    if(index < 0) {
      String serviceName = urlInfo;
      conf.set("pleiades.service.name", serviceName);
    } else {
      String serviceName = urlInfo.substring(0, index);
      conf.set("pleiades.service.name", serviceName);
      
      String property = urlInfo.substring(index + 1);
      StringTokenizer st = new StringTokenizer(property, ("&"));
      
      while(st.hasMoreTokens()) {
        String token = st.nextToken();
        index = token.indexOf("=");
        if(index <= 0) {
          throw new SQLException("Wrong url [" + token + "]");
        }
        String name = token.substring(0, index);
        String value = token.substring(index + 1);
        
        if("user".equals(name)) {
          userId = value;
          conf.setUserId(userId);
        } else if("charset".equals(name)) {
          charset = value;
        } else if("filesystem".equals(name)) {
          if(value == null || value.length() == 0) {
            continue;
          }
          if(value.startsWith("hdfs")) {
            conf.set("cloudata.filesystem", "hadoop");
            conf.set("filesystem.url", value);
          } else if(value.startsWith("local")) {
            conf.set("cloudata.filesystem", "local");
          }
        }
      }
    }    
    
    conf.set("clusterManager.port", port);
    conf.set("clusterID", clusterId);
    
    return new CloudataConnection(url, conf);
  }    

  /* (non-Javadoc)
   * @see java.sql.Driver#getMajorVersion()
   */
  @Override
  public int getMajorVersion() {
    return Constants.MAJOR_VERSION;
  }

  /* (non-Javadoc)
   * @see java.sql.Driver#getMinorVersion()
   */
  @Override
  public int getMinorVersion() {
    return Constants.MINOR_VERSION;
  }

  /* (non-Javadoc)
   * @see java.sql.Driver#getPropertyInfo(java.lang.String, java.util.Properties)
   */
  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws SQLException {
    return new DriverPropertyInfo[0];
  }

  /* (non-Javadoc)
   * @see java.sql.Driver#jdbcCompliant()
   */
  @Override
  public boolean jdbcCompliant() {
    return false;
  }

}
