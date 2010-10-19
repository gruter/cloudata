package org.cloudata.core.commitlog.pipe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.Pipe.SinkChannel;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.commitlog.pipe.Pipe.Context;
import org.cloudata.core.common.CStopWatch;


public class AsyncFileWriter {
  public static final byte ACK = (byte) 0xff;
  public static final byte NACK = (byte) 0xfd;

  static final Log LOG = LogFactory.getLog(AsyncFileWriter.class);

  private static class Entry {
    final Context ctx;
    final SinkChannel signal;
    boolean sendingAck = false;
    ByteBuffer ret = null;;
    long createTime = System.currentTimeMillis();

    public Entry(Context ctx) {
      this.ctx = ctx;
      this.signal = ctx.signalSender;
    }
  }

  final ExecutorService threadPool = Executors.newCachedThreadPool();
  final LinkedList<Entry> queue = new LinkedList<Entry>();

  public AsyncFileWriter() {
  }

  public void schedule(Context ctx) {
    threadPool.execute(createWritingRunnable(ctx));
  }

  public void start() {
    LOG.debug("AsyncFileWriter starts");
  }

  public void stop() {
    threadPool.shutdown();
    LOG.debug("AsyncFileWriter finishes");
  }

  private Runnable createWritingRunnable(final Context ctx) {
    return new Runnable() {
      public void run() {
        try {
          CommitLogFileChannel ch = null;

          try {
            CStopWatch watch = new CStopWatch();
            watch.start("AsyncFileWriter.openChannel", 100);
            ch = CommitLogFileChannel.openChannel(ctx.bulk.getDirName(),
                ctx.logPath, ctx.fileName);
            watch.stopAndReportIfExceed(LOG);
          } catch (IOException e) {
            ctx.writingError = e;
            signal(ctx, NACK);

            if (e instanceof ClosedByInterruptException) {
              LOG.info("Writing thread is done, " + e);
              return;
            }

            return;
          } catch (Exception e) {
            ctx.writingError = new IOException("nested exception", e);
            signal(ctx, NACK);
            return;
          }

          try {
            long t1 = System.currentTimeMillis();
            ch.write(ctx.bulk.getBufferArray());
            long t2 = System.currentTimeMillis();
//            if ((t2 - t1) >= 1000) {
//              ByteBuffer[] bufferList = ctx.bulk.getBufferArray();
//              long totalSize = 0;
//              for(ByteBuffer buf : bufferList) {
//                totalSize += buf.limit();
//              }
//                
//              LOG.info("TIME REPORT write file " + ctx.bulk.getDirName() + ", size : " + totalSize + " bytes, time : " + (t2 - t1) + " ms");
//            }
            ctx.writingError = null;
            signal(ctx, ACK);
          } catch (IOException e) {
            ctx.writingError = e;
            signal(ctx, NACK);

            if (e instanceof ClosedByInterruptException) {
              LOG.info("Writing thread is done, " + e);
              return;
            }
          } finally {
            try {
              CommitLogFileChannel.closeChannel(ch);
            } catch (IOException e) {
              LOG.warn("closing channel error : " + e);
            }
          }
        } catch (Exception e) {
          LOG.warn("Exception in writing thread, but continue", e);
        }
      }

      private void signal(Context ctx, byte ret) throws InterruptedException {
        ByteBuffer buf = ByteBuffer.allocate(9);
        buf.put(ret);
        buf.putLong(System.currentTimeMillis());
        buf.flip();
        LOG.debug("send signal " + ctx.getPipeKey());
        try {
          while(buf.hasRemaining()) {
            ctx.signalSender.write(buf);
            if (buf.hasRemaining()) {
              Thread.sleep(10);
            }
          }
        } catch (IOException e1) {
          LOG.warn("Writing return value error", e1);
        }
      }
    };
  }
}
