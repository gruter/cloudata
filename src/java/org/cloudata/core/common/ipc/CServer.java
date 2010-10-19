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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;
import org.cloudata.core.common.metrics.RPCServerMetrics;
import org.cloudata.core.common.util.ReflectionUtils;
import org.cloudata.core.common.util.StringUtils;


/** An abstract IPC service.  IPC calls take a single {@link CWritable} as a
 * parameter, and return a {@link CWritable} as their value.  A service runs on
 * a port and is defined by a parameter class and a value class.
 * 
 * @author Doug Cutting
 * @see CClient
 */
public abstract class CServer {
  
	//static long beforeProcess = 0;
  /**
   * The first four bytes of Hadoop RPC connections
   */
  public static final ByteBuffer HEADER = ByteBuffer.wrap("nrpc".getBytes());
  
  /**
   * How much time should be allocated for actually running the handler?
   * Calls that are older than ipc.timeout * MAX_CALL_QUEUE_TIME
   * are ignored when the handler takes them off the queue.
   */
  //private static final float MAX_CALL_QUEUE_TIME = 0.6f;
  
  /**
   * How many calls/handler are allowed in the queue.
   */
  private static final int MAX_QUEUE_SIZE_PER_HANDLER = 200;
  
  public static final Log LOG =
    LogFactory.getLog("org.apache.hadoop.ipc.Server");

  private static final ThreadLocal<CServer> SERVER = new ThreadLocal<CServer>();

  /** Returns the server instance called under or null.  May be called under
   * {@link #call(CWritable)} implementations, and under {@link CWritable}
   * methods of paramters and return values.  Permits applications to access
   * the server context.*/
  public static CServer get() {
    return SERVER.get();
  }
 
  /** This is set to Call object before Handler invokes an RPC and reset
   * after the call returns.
   */
  private static final ThreadLocal<Call> CurCall = new ThreadLocal<Call>();
  
  /** Returns the remote side ip address when invoked inside an RPC 
   *  Returns null incase of an error.
   */
  public static InetAddress getRemoteIp() {
    Call call = CurCall.get();
    if (call != null) {
      return call.connection.socket.getInetAddress();
    }
    return null;
  }
  /** Returns remote address as a string when invoked inside an RPC.
   *  Returns null in case of an error.
   */
  public static String getRemoteAddress() {
    InetAddress addr = getRemoteIp();
    return (addr == null) ? null : addr.getHostAddress();
  }
  
  private String bindAddress; 
  private int port;                               // port we listen on
  private int handlerCount;                       // number of handler threads
  private Class paramClass;                       // class of call parameters
  private int maxIdleTime;                        // the maximum idle time after 
                                                  // which a client may be disconnected
  private int thresholdIdleConnections;           // the number of idle connections
                                                  // after which we will start
                                                  // cleaning up idle 
                                                  // connections
  int maxConnectionsToNuke;                       // the max number of 
                                                  // connections to nuke
                                                  //during a cleanup
  
  private BlockingQueue<Call> callQueue;   
  
  private CloudataConf conf;

//  private int timeout;
  //private long maxCallStartAge;
  private int maxQueueSize;

  volatile private boolean running = true;         // true while server runs
  //private LinkedList<Call> callQueue = new LinkedList<Call>(); // queued calls

  private List<Connection> connectionList = 
    Collections.synchronizedList(new LinkedList<Connection>());
  //maintain a list
  //of client connections
  private Listener listener = null;
  private Responder responder = null;
  private int numConnections = 0;
  private Handler[] handlers = null;

  private RPCServerMetrics serverMetrics;
  
  /**
   * A convience method to bind to a given address and report 
   * better exceptions if the address is not a valid host.
   * @param socket the socket to bind
   * @param address the address to bind to
   * @param backlog the number of connections allowed in the queue
   * @throws BindException if the address can't be bound
   * @throws UnknownHostException if the address isn't a valid host name
   * @throws IOException other random errors from bind
   */
  static void bind(ServerSocket socket, InetSocketAddress address, 
                   int backlog) throws IOException {
    try {
      socket.bind(address, backlog);
    } catch (BindException e) {
      throw new BindException("Problem binding to " + address + "," + e.getMessage());
    } catch (SocketException e) {
      // If they try to bind to a different host's address, give a better
      // error message.
      if ("Unresolved address".equals(e.getMessage())) {
        throw new UnknownHostException("Invalid hostname for server: " + 
                                       address.getHostName());
      } else {
        throw e;
      }
    }
  }

  /** A call queued for handling. */
  private static class Call {
    private int id;                               // the client's call id
    private CWritable param;                       // the parameter passed
    private Connection connection;                // connection to client
    //private long receivedTime;                    // the time received
    private long timestamp;

    private ByteBuffer response;    
    
    public Call(int id, CWritable param, Connection connection) {
      this.id = id;
      this.param = param;
      this.connection = connection;
      this.timestamp = System.currentTimeMillis();
      this.response = null;
    }
    
    public String toString() {
      return param.toString() + " from " + connection.toString();
    }
    public void setResponse(ByteBuffer response) {
      this.response = response;
    }
  }

  /** Listens on the socket. Creates jobs for the handler threads*/
  private class Listener extends Thread {
    
    private ServerSocketChannel acceptChannel = null; //the accept channel
    private Selector selector = null; //the selector that we use for the server
    private InetSocketAddress address; //the address we bind at
    private Random rand = new Random();
    private long lastCleanupRunTime = 0; //the last time when a cleanup connec-
                                         //-tion (for idle connections) ran
    private long cleanupInterval = 10000; //the minimum interval between 
                                          //two cleanup runs
    private int backlogLength = conf.getInt("ipc.server.listen.queue.size", 128);
    
    public Listener() throws IOException {
      address = new InetSocketAddress(bindAddress, port);
      // Create a new server socket and set to non blocking mode
      acceptChannel = ServerSocketChannel.open();
      acceptChannel.configureBlocking(false);

      // Bind the server socket to the local host and port
      bind(acceptChannel.socket(), address, backlogLength);
      port = acceptChannel.socket().getLocalPort(); //Could be an ephemeral port
      // create a selector;
      selector= Selector.open();

      // Register accepts on the server socket with the selector.
      acceptChannel.register(selector, SelectionKey.OP_ACCEPT);
      this.setName("IPC Server listener on " + port);
      this.setDaemon(true);
    }
    /** cleanup connections from connectionList. Choose a random range
     * to scan and also have a limit on the number of the connections
     * that will be cleanedup per run. The criteria for cleanup is the time
     * for which the connection was idle. If 'force' is true then all 
     * connections will be looked at for the cleanup.
     */
    private void cleanupConnections(boolean force) {
      if (force || numConnections > thresholdIdleConnections) {
        long currentTime = System.currentTimeMillis();
        if (!force && (currentTime - lastCleanupRunTime) < cleanupInterval) {
          return;
        }
        int start = 0;
        int end = numConnections - 1;
        if (!force) {
          start = rand.nextInt() % numConnections;
          end = rand.nextInt() % numConnections;
          int temp;
          if (end < start) {
            temp = start;
            start = end;
            end = temp;
          }
        }
        int i = start;
        int numNuked = 0;
        while (i <= end) {
          Connection c;
          synchronized (connectionList) {
            try {
              c = connectionList.get(i);
            } catch (Exception e) {return;}
          }
          if (c.timedOut(currentTime)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
              }
            closeConnection(c);              
            numNuked++;
            end--;
            c = null;
            if (!force && numNuked == maxConnectionsToNuke) break;
          }
          else i++;
        }
        lastCleanupRunTime = System.currentTimeMillis();
      }
    }

    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(CServer.this);
      //long time1 = 0;
      //long time2 = 0;
      while (running) {
        SelectionKey key = null;
        try {
          //time1 = System.nanoTime();
          //System.out.println(">>>" + (time1-time2));
          selector.select();
          //time2 = System.nanoTime();
          Iterator iter = selector.selectedKeys().iterator();
          
          while (iter.hasNext()) {
            key = (SelectionKey)iter.next();
            iter.remove();
            try {
              if (key.isValid()) {
                if (key.isAcceptable())
                  doAccept(key);
                else if (key.isReadable())
                  doRead(key);
              }
            } catch (IOException e) {
              LOG.warn("exception in processing select", e);
              key.cancel();
            }
            key = null;
          }
        } catch (OutOfMemoryError e) {
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give 
          // some thread(s) a chance to finish
          LOG.warn("Out of Memory in server select", e);
          closeCurrentConnection(key, e);
          cleanupConnections(true);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          closeCurrentConnection(key, e);
        }
        cleanupConnections(false);
      }
      LOG.info("Stopping " + this.getName());

      synchronized (this) {
        try {
          acceptChannel.close();
          selector.close();
        } catch (IOException e) { }

        selector= null;
        acceptChannel= null;
        connectionList = null;
      }
    }

    private void closeCurrentConnection(SelectionKey key, Throwable e) {
      if (key != null) {
        Connection c = (Connection)key.attachment();
        if (c != null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": disconnecting client " + c.getHostAddress());
          }
          closeConnection(c);
          c = null;
        }
      }
    }

    InetSocketAddress getAddress() {
      return new InetSocketAddress(acceptChannel.socket().getInetAddress(), acceptChannel.socket().getLocalPort());
    }
    
    void doAccept(SelectionKey key) throws IOException,  OutOfMemoryError {
      Connection c = null;
      ServerSocketChannel server = (ServerSocketChannel) key.channel();
      for (int i=0; i<10; i++) {
        server.configureBlocking(false);
        SocketChannel channel = server.accept();
		if (channel == null) return;

        channel.configureBlocking(false);
        channel.socket().setTcpNoDelay(false);
        SelectionKey readKey = channel.register(selector, SelectionKey.OP_READ);
        c = new Connection(readKey, channel, System.currentTimeMillis());
        readKey.attach(c);
        synchronized (connectionList) {
          connectionList.add(numConnections, c);
          numConnections++;
        }
//        LOG.fatal("Server connection from " + c.toString() +
//            "; # active connections: " + numConnections +
//            "; # queued calls: " + callQueue.size());
        if (LOG.isDebugEnabled())
          LOG.debug("Server connection from " + c.toString() +
                    "; # active connections: " + numConnections +
                    "; # queued calls: " + callQueue.size());
      }
    }

    void doRead(SelectionKey key) throws InterruptedException {
      int count = 0;
      Connection c = (Connection)key.attachment();
      if (c == null) {
        return;  
      }
      c.setLastContact(System.currentTimeMillis());
      
      try {
        count = c.readAndProcess();
      } catch (InterruptedException ieo) {
        throw ieo; 
      } catch (Exception e) {
        key.cancel();
        if(count != 0) {
          LOG.fatal(getName() + ": readAndProcess threw exception " + e + ". Count of bytes read: " + count, e);
        }
        count = -1; //so that the (count < 0) block is executed
      }
      //LOG.fatal("endRead:count=" + count);
      if (count < 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(getName() + ": disconnecting client " + 
                    c.getHostAddress() + ". Number of active connections: "+
                    numConnections);
        }
        closeConnection(c);
        c = null;
      }
      else {
        c.setLastContact(System.currentTimeMillis());
      }
    }   

    synchronized void doStop() {
      if (selector != null) {
        selector.wakeup();
        Thread.yield();
      }
      if (acceptChannel != null) {
        try {
          acceptChannel.socket().close();
        } catch (IOException e) {
          LOG.info(getName() + ":Exception in closing listener socket. " + e);
        }
      }
    }
  }

  /** Reads calls from a connection and queues them for handling. */
  private class Connection {
    private boolean firstData = true;
    private SocketChannel channel;
//    private SelectionKey key;
    private ByteBuffer data;
    private ByteBuffer dataLengthBuffer;
    private LinkedList<Call> responseQueue;
//    private DataOutputStream out;
//    private SocketChannelOutputStream channelOut;
    private long lastContact;
    private int dataLength;
    private Socket socket;
    // Cache the remote host & port info so that even if the socket is 
    // disconnected, we can say where it used to connect to.
    private String hostAddress;
    private int remotePort;

    public Connection(SelectionKey key, SocketChannel channel, 
                      long lastContact) {
//      this.key = key;
      this.channel = channel;
      this.lastContact = lastContact;
      this.data = null;
      this.dataLengthBuffer = ByteBuffer.allocate(4);
      this.socket = channel.socket();
//      this.out = new DataOutputStream
//        (new BufferedOutputStream(
//          this.channelOut = new SocketChannelOutputStream(channel)));
      InetAddress addr = socket.getInetAddress();
      if (addr == null) {
        this.hostAddress = "*Unknown*";
      } else {
        this.hostAddress = addr.getHostAddress();
      }
      this.remotePort = socket.getPort();
      this.responseQueue = new LinkedList<Call>();
    }   

    public String toString() {
      return getHostAddress() + ":" + remotePort; 
    }
    
    public String getHostAddress() {
      return hostAddress;
    }

    public void setLastContact(long lastContact) {
      this.lastContact = lastContact;
    }

    public long getLastContact() {
      return lastContact;
    }

    private boolean timedOut(long currentTime) {
      if (currentTime -  lastContact > maxIdleTime)
        return true;
      return false;
    }
   

    public int readAndProcess() throws IOException, InterruptedException {
		//beforeProcess = System.nanoTime();
      int count = -1;
      if (dataLengthBuffer.remaining() > 0) {
        count = channel.read(dataLengthBuffer);       
        if (count < 0 || dataLengthBuffer.remaining() > 0) 
          return count;        
        dataLengthBuffer.flip(); 
        // Is this a new style header?
        if (firstData && HEADER.equals(dataLengthBuffer)) {
          // If so, read the version
          ByteBuffer versionBuffer = ByteBuffer.allocate(1);
          count = channel.read(versionBuffer);
          if (count < 0) {
            return count;
          }
          // read the first length
          dataLengthBuffer.clear();
          count = channel.read(dataLengthBuffer);
          if (count < 0 || dataLengthBuffer.remaining() > 0) {
            return count;
          }
          dataLengthBuffer.flip();
          firstData = false;
        }
        dataLength = dataLengthBuffer.getInt();
        data = ByteBuffer.allocate(dataLength);
      }
      count = channel.read(data);
      if (data.remaining() == 0) {
        data.flip();
		//long before = System.nanoTime();
        processData();
		//System.out.println("processData() : " + ((System.nanoTime() - before) / 1000000.0));
        dataLengthBuffer.flip();
        data = null; 
      }
      return count;
    }

	// sangchul
	// it takes almost 0.1 ms for processData() function to be executed.
    private void processData() throws  IOException, InterruptedException {
      DataInputStream dis =
        new DataInputStream(new ByteArrayInputStream(data.array()));
      int id = dis.readInt();                    // try to read an id
        
      if (LOG.isDebugEnabled())
        LOG.debug(" got #" + id);
        
      CWritable param = null;
      try {
        param = (CWritable)ReflectionUtils.newInstance(paramClass, conf);           // read param
        param.readFields(dis);        
      } catch(IOException e) {
        LOG.error("IOException in readFields of parameters", e);
        throw e;
      } catch(Exception e) {
        LOG.error("Exception in readFields of parameters", e);
        throw new IOException(e.getMessage());
      }        
        
      Call call = new Call(id, param, this);
      callQueue.put(call);              // queue the call
      if(serverMetrics != null) {
        serverMetrics.incrementCallQueue();
      }
    }

    private synchronized void close() throws IOException {
      data = null;
      dataLengthBuffer = null;
      if (!channel.isOpen())
        return;
      try {socket.shutdownOutput();} catch(Exception e) {}
//      try {out.close();} catch(Exception e) {}
//      try {channelOut.destroy();} catch(Exception e) {}
      if (channel.isOpen()) {
        try {channel.close();} catch(Exception e) {}
      }
      try {socket.close();} catch(Exception e) {}
//      try {key.cancel();} catch(Exception e) {}
//      key = null;
    }
  }
  
  AtomicInteger numWaitHandlers = new AtomicInteger(0);
  long starvationTime = 0;
  
  private Call takeCallFromQueue() throws InterruptedException {
    int numHandlers = numWaitHandlers.getAndIncrement(); 
    if (numHandlers == 0 && starvationTime > 0) {
      long s = System.currentTimeMillis() - starvationTime;
      if (s > 1000) {
        LOG.warn("TIME REPORT RPC Handler starvation time : " + s + "ms");
      }
    }
    try {
      Call call = callQueue.take();
      if(serverMetrics != null) {
        serverMetrics.decrementCallQueue();
      }
      return call;
    } finally {
      if (numWaitHandlers.decrementAndGet() == 0) {
        starvationTime = System.currentTimeMillis();
      }
    }
  }

  /** Handles queued calls . */
  private class Handler extends Thread {
    public Handler(int instanceNumber) {
      this.setDaemon(true);
      this.setName("IPC Server handler "+ instanceNumber + " on " + port);
    }

    public void run() {
      LOG.debug(getName() + ": starting");
      SERVER.set(CServer.this);
      ByteArrayOutputStream buf = new ByteArrayOutputStream(10240);
      while (running) {
        try {
          Call call = takeCallFromQueue();

          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": has #" + call.id + " from " +
                      call.connection);
          }
          
          if(serverMetrics != null) {
            serverMetrics.incrementRunningCall();
            long waitTime = System.currentTimeMillis() - call.timestamp;
            if(waitTime > 0) {
              serverMetrics.setQueueWaitTime(System.currentTimeMillis() - call.timestamp);
            }
          }          
          
          String errorClass = null;
          String error = null;
          CWritable value = null;
          
          CurCall.set(call);
          try {
            value = call(call.param);             // make the call
          } catch (Throwable e) {
            LOG.debug(getName()+", call "+call+": error: " + e, e);
            errorClass = e.getClass().getName();
            error = StringUtils.stringifyException(e);
          }
          CurCall.set(null);
            
          //long startTime = System.currentTimeMillis();
          //DataOutputStream out = call.connection.out;
          buf.reset();
          DataOutputStream out = new DataOutputStream(buf);
          out.writeInt(call.id);                // write call id
          out.writeBoolean(error!=null);        // write error flag
          if (error == null) {
            value.write(out);
          } else {
            CWritableUtils.writeString(out, errorClass);
            CWritableUtils.writeString(out, error);
          }
          call.setResponse(ByteBuffer.wrap(buf.toByteArray()));
          responder.doRespond(call);
          //LOG.info("before : " + ((beforeCall - Server.beforeProcess) / 1000000.0) + ", call elapsed time : " + ((afterCall - beforeCall) / 1000000.0) + ", response time : " + ((System.nanoTime() - afterCall) / 1000000.0));
        } catch (InterruptedException e) {
          if (running) {                          // unexpected -- log it
            LOG.info(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } catch (Exception e) {
          if(e instanceof ClosedChannelException) {
            LOG.debug(getName() + " caught: " +
                StringUtils.stringifyException(e));
          } else {
            LOG.error(getName() + " caught: " +
                     StringUtils.stringifyException(e));
          }
        } finally {
          if(serverMetrics != null) {
            serverMetrics.decrementRunningCall();
          }          
        }
      }
      LOG.info(getName() + ": exiting");
    }
  }
  /** Constructs a server listening on the named port and address.  Parameters passed must
   * be of the named class.  The <code>handlerCount</handlerCount> determines
   * the number of handler threads that will be used to process calls.
   * 
   */
  protected CServer(String bindAddress, int port, Class paramClass, int handlerCount, 
      CloudataConf conf, RPCServerMetrics serverMetrics) 
    throws IOException {
    this.bindAddress = bindAddress;
    this.conf = conf;
    this.port = port;
    this.paramClass = paramClass;
    this.handlerCount = handlerCount;
    //maxCallStartAge = (long) (timeout * MAX_CALL_QUEUE_TIME);
    this.maxQueueSize = handlerCount * MAX_QUEUE_SIZE_PER_HANDLER;
    this.callQueue  = new LinkedBlockingQueue<Call>(maxQueueSize); 
    this.maxIdleTime = conf.getInt("ipc.client.maxidletime", 120000);
    this.maxConnectionsToNuke = conf.getInt("ipc.client.kill.max", 10);
    this.thresholdIdleConnections = conf.getInt("ipc.client.idlethreshold", 4000);
    
    // Start the listener here and let it bind to the port
    listener = new Listener();
    this.port = listener.getAddress().getPort();

    this.serverMetrics = serverMetrics;
    responder = new Responder();
  }

  private void closeConnection(Connection connection) {
    synchronized (connectionList) {
      if (connectionList.remove(connection))
        numConnections--;
    }
    try {
      connection.close();
    } catch (IOException e) {
    }
  }
  
  /** Starts the service.  Must be called before any calls will be handled. */
  public synchronized void start() throws IOException {
    responder.start();
    listener.start();
    handlers = new Handler[handlerCount];
    
    for (int i = 0; i < handlerCount; i++) {
      handlers[i] = new Handler(i);
      handlers[i].start();
    }
  }

  /** Stops the service.  No new calls will be handled after this is called. */
  public synchronized void stop() {
    LOG.info("Stopping server on " + port);
    running = false;
    if (handlers != null) {
      for (int i = 0; i < handlerCount; i++) {
        if (handlers[i] != null) {
          handlers[i].interrupt();
        }
      }
    }
    listener.interrupt();
    listener.doStop();
    responder.interrupt();
    notifyAll();
  }

  /** Wait for the server to be stopped.
   * Does not wait for all subthreads to finish.
   *  See {@link #stop()}.
   */
  public synchronized void join() throws InterruptedException {
    while (running) {
      wait();
    }
  }

  /**
   * Return the socket (ip+port) on which the RPC server is listening to.
   * @return the socket (ip+port) on which the RPC server is listening to.
   */
  public synchronized InetSocketAddress getListenerAddress() {
    return listener.getAddress();
  }
  
  /** Called for each call. */
  public abstract CWritable call(CWritable param) throws IOException;
  
  // Sends responses of RPC back to clients.
  private class Responder extends Thread {
    private Selector writeSelector;
    private int pending;         // connections waiting to register
    
    final static int PURGE_INTERVAL = 900000; // 15mins

    Responder() throws IOException {
      this.setName("IPC Server Responder");
      this.setDaemon(true);
      writeSelector = Selector.open(); // create a selector
      pending = 0;
    }

    @Override
    public void run() {
      LOG.info(getName() + ": starting");
      SERVER.set(CServer.this);
      long lastPurgeTime = 0;   // last check for old calls.

      while (running) {
        try {
          waitPending();     // If a channel is being registered, wait.
          writeSelector.select(PURGE_INTERVAL);
          Iterator<SelectionKey> iter = writeSelector.selectedKeys().iterator();
          while (iter.hasNext()) {
            SelectionKey key = iter.next();
            iter.remove();
            try {
              if (key.isValid() && key.isWritable()) {
                  doAsyncWrite(key);
              }
            } catch (IOException e) {
              LOG.info(getName() + ": doAsyncWrite threw exception " + e);
            }
          }
          long now = System.currentTimeMillis();
          if (now < lastPurgeTime + PURGE_INTERVAL) {
            continue;
          }
          lastPurgeTime = now;
          //
          // If there were some calls that have not been sent out for a
          // long time, discard them.
          //
          LOG.debug("Checking for old call responses.");
          synchronized (writeSelector.keys()) {
            iter = writeSelector.keys().iterator();
            while (iter.hasNext()) {
              SelectionKey key = iter.next();
              try {
                doPurge(key, now);
              } catch (IOException e) {
                LOG.warn("Error in purging old calls " + e);
              }
            }
          }
        } catch (OutOfMemoryError e) {
          //
          // we can run out of memory if we have too many threads
          // log the event and sleep for a minute and give
          // some thread(s) a chance to finish
          //
          LOG.warn("Out of Memory in server select", e);
          try { Thread.sleep(60000); } catch (Exception ie) {}
        } catch (Exception e) {
          LOG.warn("Exception in Responder " + 
                   StringUtils.stringifyException(e));
        }
      }
      LOG.info("Stopping " + this.getName());
    }

    private void doAsyncWrite(SelectionKey key) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        throw new IOException("doAsyncWrite: bad channel");
      }

      synchronized(call.connection.responseQueue) {
        if (processResponse(call.connection.responseQueue, false)) {
          try {
            key.interestOps(0);
          } catch (CancelledKeyException e) {
            /* The Listener/reader might have closed the socket.
             * We don't explicitly cancel the key, so not sure if this will
             * ever fire.
             * This warning could be removed.
             */
            LOG.warn("Exception while changing ops : " + e);
          }
        }
      }
    }

    //
    // Remove calls that have been pending in the responseQueue 
    // for a long time.
    //
    private void doPurge(SelectionKey key, long now) throws IOException {
      Call call = (Call)key.attachment();
      if (call == null) {
        return;
      }
      if (key.channel() != call.connection.channel) {
        LOG.info("doPurge: bad channel");
        return;
      }
      LinkedList<Call> responseQueue = call.connection.responseQueue;
      synchronized (responseQueue) {
        Iterator<Call> iter = responseQueue.listIterator(0);
        while (iter.hasNext()) {
          call = iter.next();
          if (now > call.timestamp + PURGE_INTERVAL) {
            closeConnection(call.connection);
            break;
          }
        }
      }
    }

    // Processes one response. Returns true if there are no more pending
    // data for this channel.
    //
    private boolean processResponse(LinkedList<Call> responseQueue,
                                    boolean inHandler) throws IOException {
      boolean error = true;
      boolean done = false;       // there is more data for this channel.
      int numElements = 0;
      Call call = null;
      try {
        synchronized (responseQueue) {
          //
          // If there are no items for this channel, then we are done
          //
          numElements = responseQueue.size();
          if (numElements == 0) {
            error = false;
            return true;              // no more data for this channel.
          }
          //
          // Extract the first call
          //
          call = responseQueue.removeFirst();
          SocketChannel channel = call.connection.channel;
          if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": responding to #" + call.id + " from " +
                      call.connection);
          }
          //
          // Send as much data as we can in the non-blocking fashion
          //
          int numBytes = channel.write(call.response);
          if (numBytes < 0) {
            return true;
          }
          if (!call.response.hasRemaining()) {
            if (numElements == 1) {    // last call fully processes.
              done = true;             // no more data for this channel.
            } else {
              done = false;            // more calls pending to be sent.
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote " + numBytes + " bytes.");
            }
          } else {
            //
            // If we were unable to write the entire response out, then 
            // insert in Selector queue. 
            //
            call.connection.responseQueue.addFirst(call);
            
            if (inHandler) {
              // set the serve time when the response has to be sent later
              call.timestamp = System.currentTimeMillis();
              
              incPending();
              try {
                // Wakeup the thread blocked on select, only then can the call 
                // to channel.register() complete.
                writeSelector.wakeup();
                channel.register(writeSelector, SelectionKey.OP_WRITE, call);
              } catch (ClosedChannelException e) {
                //Its ok. channel might be closed else where.
                done = true;
              } finally {
                decPending();
              }
            }
            if (LOG.isDebugEnabled()) {
              LOG.debug(getName() + ": responding to #" + call.id + " from " +
                        call.connection + " Wrote partial " + numBytes + 
                        " bytes.");
            }
          }
          error = false;              // everything went off well
        }
      } finally {
        if (error && call != null) {
          if(call.param != null && call.param.toString().indexOf("getProtocolVersion") >= 0) {
            LOG.debug(getName()+", call " + call + ": output error");
          } else {
            LOG.warn(getName()+", call " + call + ": output error");
          }
          done = true;               // error. no more data for this channel.
          closeConnection(call.connection);
        }
      }
      return done;
    }

    //
    // Enqueue a response from the application.
    //
    void doRespond(Call call) throws IOException {
      synchronized (call.connection.responseQueue) {
        call.connection.responseQueue.addLast(call);
        //LOG.fatal("call.connection.responseQueue.size():" + call.connection.responseQueue.size());
        if (call.connection.responseQueue.size() == 1) {
          processResponse(call.connection.responseQueue, true);
        }
      }
    }

    private synchronized void incPending() {   // call waiting to be enqueued.
      pending++;
    }

    private synchronized void decPending() { // call done enqueueing.
      pending--;
      notify();
    }

    private synchronized void waitPending() throws InterruptedException {
      while (pending > 0) {
        wait();
      }
    }
  }
}
