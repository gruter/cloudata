package org.cloudata.core.commitlog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.cloudata.core.commitlog.CommitLogClient;
import org.cloudata.core.common.conf.CloudataConf;


public class TestScalability {
  static int writtenLength = 0;
  static int dirNo = 1;
  static final int MAX_DIR_SIZE = 1024 * 1024 * 10; // 10 MB
  static final int TEST_DIR_COUNT = 100;

  public static void main(String[] args) {
    CloudataConf conf = new CloudataConf();

    InetSocketAddress[] addrList = new InetSocketAddress[] {
        new InetSocketAddress("host01", 57001), new InetSocketAddress("host02", 57001),
        new InetSocketAddress("host03", 57001) };

    try {
      CommitLogClient client = new CommitLogClient(conf, addrList);
      client.open();

      byte[] data = createData();

      long start = System.currentTimeMillis();
      long commitElapsed = 0;
      int commitCount = 0;

      System.out.println("Start : " + new Date(start));
      while (dirNo < TEST_DIR_COUNT) {
        writtenLength += data.length;

        if (writtenLength >= MAX_DIR_SIZE) {
          long current = System.currentTimeMillis();
          long elapsed = current - start;
          System.out.println("directory_" + dirNo + " : " + elapsed + " ms, speed : "
              + ((double) MAX_DIR_SIZE / (double) elapsed / 1000.0) + " MB/s, avg commit : "
              + ((double) commitElapsed / commitCount) + " ms");
          dirNo++;
          writtenLength = 0;
          start = current;
          commitElapsed = 0;
          commitCount = 0;
        }

        String dirName = "directory_" + (Math.abs(new Random().nextInt() % dirNo));

        long before = System.currentTimeMillis();
        client.startWriting(commitCount + 1, dirName);
        client.write(data);
        client.commitWriting();
        commitElapsed += System.currentTimeMillis() - before;
        commitCount++;
      }

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static byte[] createData() {
    byte[] data = new byte[1000];
    Arrays.fill(data, (byte) 0xff);
    return data;
  }
}
