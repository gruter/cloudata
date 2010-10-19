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
package org.cloudata.core.rest;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.BoundedThreadPool;

import com.noelios.restlet.ext.servlet.ServerServlet;

/**
 * @author doe-nam kim
 *
 */
public class RestWebServer {
  private static final Log LOG = LogFactory.getLog(RestWebServer.class.getName());
  
  private static int numOfThreads = 100;
  private static Server jettyServer = null;
  SelectChannelConnector jettyConnector = null;
  
  public static void main(String[] args) {
    try {
      if(args.length >= 1 && args[0].equalsIgnoreCase("-help")) {
        System.out.println("usage:  RestWebServer [portno]");
        System.out.println("note: if no portno defined, " +
                           "defaults to value in cloudata-default.xml or cloudata-site.xml");
        System.exit(0);
      }
      
      if(args.length != 0) {
        (new RestWebServer()).startWebServer(Integer.parseInt(args[0]));
      } else {
        (new RestWebServer()).startWebServer(0);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      System.exit(0);
    }
  }
  
  public void startWebServer(int port) throws Exception  {
    CloudataConf conf = new CloudataConf();
    int portNum = conf.getInt("rest.server.port", 7003);
    numOfThreads = conf.getInt("rest.server.http.threads", 100);
    
    if(port != 0) {
      portNum = port;
    }
    
    //set up jetty connector
    jettyConnector = new SelectChannelConnector();
    jettyConnector.setLowResourcesConnections(numOfThreads - 10);
    jettyConnector.setLowResourceMaxIdleTime(1500);
    jettyConnector.setPort(portNum);
    
    jettyConnector.setHost(InetAddress.getLocalHost().getHostName());
    
    //set up jetty server
    jettyServer = new Server(portNum);
    jettyServer.setConnectors(new Connector[]{ jettyConnector});
    BoundedThreadPool pool = new BoundedThreadPool();
    pool.setMaxThreads(numOfThreads);
    jettyServer.setThreadPool(pool);
      //and add the servlet to it
    Context root = new Context(jettyServer, "/", Context.SESSIONS);
    
    Map<String, String> initParams = new HashMap<String, String>();
    initParams.put("org.restlet.application", RestApplication.class.getName());
    root.setInitParams(initParams);
    //root.addServlet(new ServletHolder(new OpenAPIServlet(conf)), "/*");
    root.addServlet(new ServletHolder(new ServerServlet()), "/*");
    
    jettyServer.start();
    
    jettyServer.setStopAtShutdown(true);
   
    LOG.info("Started Cloudata REST HTTP Server on port number " + portNum);
  }
}
