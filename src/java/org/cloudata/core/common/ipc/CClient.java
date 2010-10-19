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

package org.cloudata.core.common.ipc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CDataOutputBuffer;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.util.NetUtils;
import org.cloudata.core.common.util.ReflectionUtils;


/**
 * A client for an IPC service. IPC calls take a single {@link CWritable} as a
 * parameter, and return a {@link CWritable} as their value. A service runs on a
 * port and is defined by a parameter class and a value class.
 * 
 * @author Doug Cutting
 * @see CServer
 */
public class CClient {
  /** Should the client send the header on the connection? */
  private static final boolean SEND_HEADER = true;

  private static final byte CURRENT_VERSION = 0;

  public static int READ_TIMEOUT = 60 * 1000;

  public static final Log LOG = LogFactory.getLog(CClient.class.getName());

  private Hashtable<InetSocketAddress, LinkedList<Connection>> connections = new Hashtable<InetSocketAddress, LinkedList<Connection>>();

  private Class valueClass; // class of call values

  private int timeout;// timeout for calls

  private int counter; // counter for call ids

  private boolean running = true; // true while client runs

  private CloudataConf conf;

  private int maxIdleTime; // connections will be culled if it was idle for

  // maxIdleTime msecs
  private int maxRetries; // the max. no. of retries for socket connections

  private SocketFactory socketFactory; // how to create sockets

  public Random random = new Random();

  protected ExecutorService parallelCallExecutor = Executors
      .newFixedThreadPool(50);

  /** A call waiting for a value. */
  private class Call {
    int id; // call id

    CWritable param; // parameter

    CWritable value; // value, null if error

    String error; // exception, null if value

    String errorClass; // class of exception

    long lastActivity; // time of last i/o

    boolean done; // true when call is done

    protected Call(CWritable param) {
      this.param = param;
      synchronized (CClient.this) {
        this.id = counter++;
      }
      touch();
    }

    /**
     * Called by the connection thread when the call is complete and the value
     * or error string are available. Notifies by default.
     */
//    public synchronized void callComplete() {
//      notify(); // notify caller
//    }

    /** Update lastActivity with the current time. */
    public synchronized void touch() {
      lastActivity = System.currentTimeMillis();
    }

    /** Update lastActivity with the current time. */
    public synchronized void setResult(CWritable value, String errorClass,
        String error) {
      this.value = value;
      // if(error != null) {
      // LOG.error("RPC error:" + error);
      // }
      this.error = error;
      this.errorClass = errorClass;
      this.done = true;
    }

  }

  /**
   * Thread that reads responses and notifies callers. Each connection owns a
   * socket connected to a remote address. Calls are multiplexed through this
   * socket: responses may be delivered out of order.
   */
  private class Connection { //extends Thread {
    private InetSocketAddress address; // address of server

    private Socket socket = null; // connected socket

    private DataInputStream in;

    private DataOutputStream out;

    // currently active calls
    private Hashtable<Integer, Call> calls = new Hashtable<Integer, Call>();

    private AtomicLong lastActivity = new AtomicLong(0);

    private String name;
    
    public Connection(InetSocketAddress address) throws IOException {
      if (address.isUnresolved()) {
        throw new UnknownHostException("unknown host: " + address.getHostName());
      }
      this.address = address;
      this.setName("IPC Client connection to " + address.toString());
      //this.setDaemon(true);
      //LOG.warn("Created Connection:" + address + "," + this);
    }

    public void setName(String name) {
      this.name = name;
    }
    
    public String getName() {
      return this.name;
    }
    
    public void setupIOstreams() throws IOException {
      if (this.socket != null) {
        return;
      }

      short failures = 0;
      while (true) {
        try {
          this.socket = socketFactory.createSocket();
          this.socket.connect(address, READ_TIMEOUT);
          break;
        } catch (IOException ie) { // SocketTimeoutException is also caught
          if (failures == maxRetries) {
            // reset inUse so that the culler gets a chance to throw this
            // connection object out of the table. We don't want to increment
            // inUse to infinity (everytime getConnection is called inUse is
            // incremented)!
            // set socket to null so that the next call to setupIOstreams
            // can start the process of connect all over again.
            socket.close();
            socket = null;
            throw new SocketTimeoutException("Can't connect to " + address
                + "," + ie.getMessage());
          }
          failures++;
          LOG.debug("Retrying connect to server: " + address
              + ". Already tried " + failures + " time(s).");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException iex) {
          }
        }
      }
      
      socket.setSoTimeout(timeout);

      this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
      this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));

      if (SEND_HEADER) {
        out.write(CServer.HEADER.array());
        out.write(CURRENT_VERSION);
      }
    }

    private void touch() {
      lastActivity.getAndSet(System.currentTimeMillis());
    }

    public boolean isIdle() {
      // check whether the connection is in use or just created
      long currTime = System.currentTimeMillis();
      if (currTime - lastActivity.getAndAdd(0) > maxIdleTime)
        return true;
      return false;
    }

    public InetSocketAddress getRemoteAddress() {
      return address;
    }

    /**
     * Initiates a call by sending the parameter to the remote server. Note:
     * this is not called from the Connection thread, but by other threads.
     */
    public void sendParam(Call call) throws IOException {
      calls.put(call.id, call);
      if (LOG.isDebugEnabled())
        LOG.debug(getName() + " sending #" + call.id);
      CDataOutputBuffer d = new CDataOutputBuffer(); // for serializing
                                                     // the
      // data to be written
      d.writeInt(call.id);
      call.param.write(d);
      byte[] data = d.getData();
      int dataLength = d.getLength();

      // long before = System.nanoTime();
      out.writeInt(dataLength); // first put the data length
      out.write(data, 0, dataLength);// write the data
      out.flush();
    }

//    public void sendParam(Call call, byte[] data, int dataLength)
//        throws IOException {
//      boolean error = true;
//      try {
//        calls.put(call.id, call);
//        synchronized (out) {
//          if (LOG.isDebugEnabled())
//            LOG.debug(getName() + " sending #" + call.id);
//
//          //first put the data length
//          out.writeInt(dataLength + NWritableUtils.getIntByteSize()); 
//          out.writeInt(call.id);
//          out.write(data, 0, dataLength);// write the data
//          out.flush();
//        }
//
//        error = false;
//      } finally {
//        if (error) {
//          LOG.debug("sendParam 2 error");
//          closeThisConnection(addess, conn);
//        }
//      }
//    }

    /** Close the connection. */
    public void close() {
      // socket may be null if the connection could not be established to the
      // server in question, and the culler asked us to close the connection
      if (socket == null)
        return;
      try {
        socket.close(); // close socket
		socket = null;
      } catch (IOException e) {
      }
      LOG.debug(getName() + ": closing");
    }
  }

  /** Call implementation used for parallel calls. */
  private class ParallelCall extends Call {
    private ParallelResults results;

    private int index;

    public ParallelCall(CWritable param, ParallelResults results, int index) {
      super(param);
      this.results = results;
      this.index = index;
    }

    /** Deliver result to result collector. */
    public void callComplete() {
      results.callComplete(this);
    }
  }

  /** Result collector for parallel calls. */
  private static class ParallelResults {
    private CWritable[] values;

    private int size;

    private int count;

    public ParallelResults(int size) {
      this.values = new CWritable[size];
      this.size = size;
    }

    /** Collect a result. */
    public synchronized void callComplete(ParallelCall call) {
      values[call.index] = call.value; // store the value
      count++; // count it
      if (count == size) // if all values are in
        notify(); // then notify waiting caller
    }
  }

  private class ConnectionCuller extends Thread {

    public static final int MIN_SLEEP_TIME = 1000;

    public void run() {

      LOG.debug(getName() + ": starting");

      while (running) {
        try {
          Thread.sleep(MIN_SLEEP_TIME);
        } catch (InterruptedException ie) {
        }

        synchronized (connections) {
          List<InetSocketAddress> removeTarget = new ArrayList<InetSocketAddress>();
          for(Map.Entry<InetSocketAddress, LinkedList<Connection>> entry: connections.entrySet()) {
            LinkedList<Connection> value = entry.getValue();
            
            Iterator<Connection> it = value.iterator();
            while(it.hasNext()) {
              Connection eachConnection = it.next();
              if (eachConnection.isIdle()) {
                it.remove();
                eachConnection.close();
              }
            }
            if(value.size() == 0) {
              removeTarget.add(entry.getKey());
            }
          }
          
          for(InetSocketAddress eachAddress: removeTarget) {
            connections.remove(eachAddress);
          }
        }
      }
    }
  }

  /**
   * Construct an IPC client whose values are of the given {@link CWritable}
   * class.
   */
  public CClient(Class valueClass, CloudataConf conf, SocketFactory factory) {
    this.valueClass = valueClass;
    this.timeout = conf.getInt("ipc.client.timeout", 10000);
    this.maxIdleTime = conf.getInt("ipc.client.connection.maxidletime", 1000);
    this.maxRetries = conf.getInt("ipc.client.connect.max.retries", 10);
    this.conf = conf;
    this.socketFactory = factory;
    Thread t = new ConnectionCuller();
    t.setDaemon(true);
    t.setName(valueClass.getName() + " Connection Culler");
    LOG.debug(valueClass.getName() + "Connection culler maxidletime= "
        + maxIdleTime + "ms");
    t.start();
  }

  /**
   * Construct an IPC client with the default SocketFactory
   * 
   * @param valueClass
   * @param conf
   */
  public CClient(Class<?> valueClass, CloudataConf conf) {
    this(valueClass, conf, NetUtils.getDefaultSocketFactory(conf));
  }

  /**
   * Stop all threads related to this client. No further calls may be made using
   * this client.
   */
  public void stop() {
    LOG.info("Stopping client");
    running = false;
  }

  /** Sets the timeout used for network i/o. */
  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at
   * <code>address</code>, returning the value. Throws exceptions if there are
   * network problems or if the remote code threw an exception.
   */
  public CWritable call(CWritable param, InetSocketAddress address)
      throws InterruptedException, IOException {
    Connection connection = getConnection(address);

    Call call = new Call(param);

    int retryCount = 0;
    while(true) {
      try {
        connection.sendParam(call);
        break;
      } catch (IOException e) {
        if(retryCount > 5) {
          LOG.error("SendParam error:" + address + "," + e.getMessage(), e);
          throw e;
        }
        LOG.error("SendParam error:" + address + "," + e.getMessage() + ", but retry:" + retryCount);
        retryCount++;
        closeThisConnection(address, connection); // close on error
        connection = getConnection(address);
      }
    }
    
    try {
      int id;
      try {
        id = connection.in.readInt(); // try to read an id
      } catch (SocketTimeoutException e) {
        throw new SocketTimeoutException("timed out waiting for rpc response["
          + address + "]" + new Date(call.lastActivity));
      }

      boolean isError = connection.in.readBoolean(); // read if error

      if (isError) {
        String exceptionClassName = CWritableUtils.readString(connection.in);
        String exceptionMessage = CWritableUtils.readString(connection.in);
        this.returnToConnections(address, connection);
        throw new CRemoteException(exceptionClassName, exceptionMessage);
      } else {
        CWritable value = (CWritable) ReflectionUtils.newInstance(
            valueClass, conf);
        value.readFields(connection.in); // read value
        this.returnToConnections(address, connection);
        return value;
      }
    } catch (EOFException eof) {
      //LOG.warn("EOFException: id : " + connection + ", socket : " + connection.socket, eof);
      closeThisConnection(address, connection);
      throw new IOException(eof.getMessage(), eof);
    } catch (CRemoteException e) {
      throw e;
    } catch (SocketTimeoutException e) {
      throw e;
    } catch (Exception e) {
      LOG.error("Error while call:" + connection.address + "," + e.getMessage(), e);
      IOException err = new IOException(e.getMessage());
      err.initCause(e);
      
      closeThisConnection(address, connection);
      
      throw err;
    } 
    
//    try {
//      synchronized (call) {
//        connection.sendParam(call); // send the parameter
//        long wait = timeout;
//        do {
//          call.wait(wait); // wait for the result
//          wait = timeout - (System.currentTimeMillis() - call.lastActivity);
//        } while (!call.done && wait > 0);
//  
//        if (call.error != null) {
//          throw new NRemoteException(call.errorClass, call.error);
//        } else if (!call.done) {
//          // FIXME 한번 timeout이 발생하면 계속 timeout이 발생함
//          throw new SocketTimeoutException("timed out waiting for rpc response["
//              + address + "]" + new Date(call.lastActivity));
//        } else {
//          return call.value;
//        }
//      }
//    } finally {
//      connection.decrementRef();
//      returnToConnections(address, connection);      
//    }
  }

  public void onewayCall(CWritable param, InetSocketAddress address)
      throws InterruptedException, IOException {
    Connection connection = getConnection(address);
    Call call = new Call(param);
    synchronized (call) {
      connection.sendParam(call);
    }
  }

  /**
   * Makes a set of calls in parallel. Each parameter is sent to the
   * corresponding address. When all values are available, or have timed out or
   * errored, the collected results are returned in an array. The array contains
   * nulls for calls that timed out or errored.
   */
//  public NWritable[] call(NWritable[] params, InetSocketAddress[] addresses)
//      throws IOException {
//    if (addresses.length == 0)
//      return new NWritable[0];
//
//    ParallelResults results = new ParallelResults(params.length);
//    synchronized (results) {
//      byte[] data = null;
//      int dataLength = 0;
//
//      // long time1 = System.nanoTime();
//      for (int i = 0; i < params.length; i++) {
//        final ParallelCall call = new ParallelCall(params[i], results, i);
//        try {
//          if (data == null) {
//            NDataOutputBuffer d = new NDataOutputBuffer(); // for serializing
//                                                           // the
//            call.param.write(d);
//            data = d.getData();
//            dataLength = d.getLength();
//          }
//          parallelCallExecutor.execute(new SendTask(call, addresses[i], data,
//              dataLength, results));
//        } catch (IOException e) {
//          results.size--; // wait for one fewer result
//        }
//      }
//      // long time2 = System.nanoTime();
//      // System.out.println(">>>>>" + (time2 - time1));
//
//      try {
//        results.wait(timeout); // wait for all results
//      } catch (InterruptedException e) {
//      }
//
//      if (results.count == 0) {
//        throw new SocketTimeoutException("no responses");
//      } else {
//        return results.values;
//      }
//    }
//  }

//  class SendTask implements Runnable {
//    ParallelCall call;
//
//    InetSocketAddress address;
//
//    byte[] data;
//
//    int dataLength;
//
//    ParallelResults results;
//
//    public SendTask(ParallelCall call, InetSocketAddress address, byte[] data,
//        int dataLength, ParallelResults results) {
//      this.call = call;
//      this.address = address;
//      this.data = data;
//      this.dataLength = dataLength;
//      this.results = results;
//    }
//
//    public void run() {
//      try {
//        Connection connection = getConnection(address);
//        connection.sendParam(call, data, dataLength);
//      } catch (IOException e) {
//        synchronized (results) {
//          results.size--;
//        }
//      }
//    }
//  }

  public void closeThisConnection(InetSocketAddress address, Connection conn) {
    //LOG.warn("closeThisConnection:" + address + "," + this);
    synchronized (connections) {
      LinkedList<Connection> hostConnections = connections.get(address);
      if (hostConnections != null) {
        hostConnections.remove(conn);
        if(hostConnections.isEmpty()) {
          connections.remove(address);
        }
      }
    }
    conn.close();
  }
  
  /**
   * 정상적으로 call이 종료되었을 때 Pool에 넣는다.
   * @param address
   * @param conn
   */
  private void returnToConnections(InetSocketAddress address, Connection conn) {
    synchronized (connections) {
      LinkedList<Connection> hostConnections = connections.get(address);
      if (hostConnections != null) {
        LOG.debug("return connection to pool : " + conn);
        conn.touch();
        hostConnections.addFirst(conn);
      }
    }
  }
  
  /**
   * Get a connection from the pool, or create a new one and add it to the pool.
   * Connections to a given host/port are reused.
   */
  private Connection getConnection(InetSocketAddress address)
      throws IOException {
    Connection connection;

    synchronized (connections) {
      LinkedList<Connection> hostConnections = connections.get(address);

      if (hostConnections == null) {
        hostConnections = new LinkedList<Connection>();
        connections.put(address, hostConnections);
      }
      if (hostConnections.isEmpty()) {
        connection = new Connection(address);
      } else {
        connection = hostConnections.removeFirst();
      }
      connection.touch();
    }
    // we don't invoke the method below inside "synchronized (connections)"
    // block above. The reason for that is if the server happens to be slow,
    // it will take longer to establish a connection and that will slow the
    // entire system down.
    connection.setupIOstreams();
    return connection;
  }
}
