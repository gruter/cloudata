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
package org.cloudata.core.common.metrics;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author jindolk
 *
 */
public class GangliaContext31 extends GangliaContext {

  String hostName = "UNKNOWN.example.com";

  private final Log LOG = LogFactory.getLog(this.getClass());    

  public synchronized void startMonitoring() {
    try {
      hostName = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
    }
    
    try {
      datagramSocket = new DatagramSocket();
    }
    catch (SocketException se) {
      se.printStackTrace();
    }
    super.startMonitoring();
    
  }
  
  protected void emitMetric(String name, String type,  String value) throws IOException
  {
    if (name == null) {
      LOG.warn("Metric was emitted with no name.");
      return;
    } else if (value == null) {
      LOG.warn("Metric name " + name +" was emitted with a null value.");
    } else if (type == null) {
      LOG.warn("Metric name " + name + ", value " + value + " has no type.");
    }

    LOG.debug("Emitting metric " + name + ", type " + type + ", value " + 
      value + " from " + hostName);

    String units = getUnits(name);
    if (units == null) {
      LOG.warn("Metric name " + name + ", value " + value
        + " had 'null' units");
      units = "";
    }
    int slope = getSlope(name);
    int tmax = getTmax(name);
    int dmax = getDmax(name);
    offset = 0;
    String groupName = name.substring(0,name.lastIndexOf("."));

    // The following XDR recipe was done through a careful reading of
    // gm_protocol.x in Ganglia 3.1 and carefully examining the output of
    // the gmetric utility with strace.

    // First we send out a metadata message
    xdr_int(128);         // metric_id = metadata_msg
    xdr_string(hostName); // hostname
    xdr_string(name);     // metric name
    xdr_int(0);           // spoof = False
    xdr_string(type);     // metric type
    xdr_string(name);     // metric name
    xdr_string(units);    // units
    xdr_int(slope);       // slope
    xdr_int(tmax);        // tmax, the maximum time between metrics
    xdr_int(dmax);        // dmax, the maximum data value

    xdr_int(1);             /*Num of the entries in extra_value field for 
                              Ganglia 3.1.x*/
    xdr_string("GROUP");    /*Group attribute*/
    xdr_string(groupName);  /*Group value*/

    for (SocketAddress socketAddress : metricsServers) {
      DatagramPacket packet =
        new DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }

    // Now we send out a message with the actual value.
    // Technically, we only need to send out the metadata message once for
    // each metric, but I don't want to have to record which metrics we did and
    // did not send.
    offset = 0;
    xdr_int(133);         // we are sending a string value
    xdr_string(hostName); // hostName
    xdr_string(name);     // metric name
    xdr_int(0);           // spoof = False
    xdr_string("%s");     // format field
    xdr_string(value);    // metric value
        
    for (SocketAddress socketAddress : metricsServers) {
      DatagramPacket packet = 
        new DatagramPacket(buffer, offset, socketAddress);
      datagramSocket.send(packet);
    }
  }

}