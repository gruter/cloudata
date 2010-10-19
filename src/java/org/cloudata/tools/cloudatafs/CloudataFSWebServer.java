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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.CStatusHttpServer;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.NetworkUtil;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.webapp.WebAppContext;
import org.mortbay.thread.BoundedThreadPool;


/**
 * @author jindolk
 *
 */
public class CloudataFSWebServer {
  private static final Log LOG =
    LogFactory.getLog(CloudataFSWebServer.class.getName());

  private Server jettyServer;
  private WebAppContext webAppContext;

  private int numOfThreads = 20;
  
  private int port;
  
  public static CloudataConf conf = new CloudataConf();
  /**
   * Create a status server on the given port.
   * The jsp scripts are taken from src/webapps/<name>.
   * @param name The name of the server
   * @param port The port to use on the server
   * @param findPort whether the server should start at the given port and 
   *        increment by 1 until it finds a free port.
   */
  public CloudataFSWebServer(String name, String bindAddress, int port) throws IOException {
    URL appDir = null;
    try {
      appDir = getWebAppsPath();
    } catch (IOException e) {
      LOG.error(e);
      System.exit(0);
    }
    this.port = port;
    
    SelectChannelConnector jettyConnector = new SelectChannelConnector();
    jettyConnector.setLowResourcesConnections(numOfThreads - 10);
    jettyConnector.setLowResourceMaxIdleTime(1500);
    jettyConnector.setPort(port);
    
    jettyConnector.setHost(InetAddress.getLocalHost().getHostName());
    
    //set up jetty server
    jettyServer = new Server(port);
    jettyServer.setConnectors(new Connector[]{ jettyConnector});
    BoundedThreadPool pool = new BoundedThreadPool();
    pool.setMaxThreads(numOfThreads);
    jettyServer.setThreadPool(pool); 
    
    webAppContext = new WebAppContext();
    webAppContext.setContextPath("/");
    webAppContext.setResourceBase(appDir.toString());
    jettyServer.addHandler(webAppContext);
    
    webAppContext.addServlet(new ServletHolder(new FileDownloadServlet()), "/download");
    
//    Map<String, String> initParams = new HashMap<String, String>();
//    initParams.put("org.restlet.application", FileRestApplication.class.getName());
//    webAppContext.setInitParams(initParams);
//    webAppContext.addServlet(new ServletHolder(new ServerServlet()), "/cloudatafs/*");
  }

  /**
   * Set a value in the webapp context. These values are available to the jsp
   * pages as "application.getAttribute(name)".
   * @param name The name of the attribute
   * @param value The value of the attribute
   */
  public void setAttribute(String name, Object value) {
    webAppContext.setAttribute(name, value);
  }

  /**
   * Get the value in the webapp context.
   * @param name The name of the attribute
   * @return The value of the attribute
   */
  public Object getAttribute(String name) {
    return webAppContext.getAttribute(name);
  }

  /**
   * Get the pathname to the webapps files.
   * @return the pathname as a URL
   */
  private static URL getWebAppsPath() throws IOException {
    URL url = CStatusHttpServer.class.getClassLoader().getResource("webapps/cloudatafs");
    if (url == null) 
      throw new IOException("webapps not found in CLASSPATH");
    
    LOG.info("webapps: " + url.toString());
    return url;
  }

  /**
   * Start the server. Does not wait for the server to start.
   */
  public void start() throws IOException {
    if(jettyServer == null) {
      return;
    }
    try {
      jettyServer.start();
    } catch (IOException ie) {
      LOG.error(ie);
      throw ie;
    } catch (Exception e) {
      IOException ie = new IOException("Problem starting http server");
      ie.initCause(e);
      throw ie;
    }
    LOG.info("Web-server up at: " + port);
  }

  /**
   * stop the server
   */
  public void stop() throws InterruptedException {
    if(jettyServer == null) {
      return;
    }

    try {
      jettyServer.stop();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  public static void main(String[] args) throws Exception {
    CloudataConf conf = new CloudataConf();
    int port = conf.getInt("CloudataFS.webserver.port", 7004);
    InetSocketAddress infoServerAddress = NetworkUtil.getAddress(InetAddress
        .getLocalHost().getHostName()
        + ":" + port);

    String host = infoServerAddress.getHostName();
    CloudataFSWebServer webServer = new CloudataFSWebServer("cloudatafs", host, port);
    System.out.println(infoServerAddress + " started...");
    webServer.start();
  }

}
