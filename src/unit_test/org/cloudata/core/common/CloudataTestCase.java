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
package org.cloudata.core.common;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;

import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.TabletLocationCache;
import org.cloudata.core.common.aop.TestHandler;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.fs.GPath;
import org.cloudata.core.tablet.TableSchema;

import junit.framework.TestCase;


public abstract class CloudataTestCase extends TestCase {
  static {
    StaticTestEnvironment.initialize();
  }

  public static DecimalFormat df = new DecimalFormat("0000000000");

  public static final String CONTENTS = "contents";
  public static final String CONTENTS_KEY = "basic";
  public static final String CONTENTSTR = "The New York Fire Department said three firefighters and one police officer were treated on scene for minor injuries. The other 22 injured were transported to various hospitals, a fire department spokesman said."
      + "New York City Mayor Michael Bloomberg said the person who died suffered from cardiac arrest."
      + "The New York Fire Department said it received a call reporting an explosion at 5:56 p.m. More than 170 firefighters were dispatched to the site at Lexington Avenue near 41st Street.";
  public static final String ANCHOR = "anchor";
  public static final String ANCHOR_KEY = "anchornum-";
  public static final String ANCHORSTR = "The area around the site was frozen because the explosion may have released asbestos in the air, Bloomberg said."
      + "Power company ConEdison was checking the area for any asbestos and expected to have test results in Wednesday night."
      + "Andrew Troisi of the New York Office of Emergency Management said people in the area who do not need to be there should leave.";

  protected CloudataConf conf;

  protected MiniCommitLogCluster miniCommitLogCluster;

  protected CloudataTestCase(String name) {
    super(name);
    conf = new CloudataConf();
    conf.set("maxTabletSize", 1);
    conf.set("cloudata.filesystem", "local");
    conf.set("testmode", "true");
    conf.setLong("clusterManager.heartbeat.timeout", 180*1000);
  }

  protected GPath getUnitTestdir(String testName) {
    return new GPath(StaticTestEnvironment.TEST_DIRECTORY_KEY, testName);
  }

  public static TableSchema makeTestTableInfo() {
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }
    return makeTestTable("Unit_Test_" + System.currentTimeMillis());
  }

  public static TableSchema makeTestTable(String tableName) {
    TableSchema table = new TableSchema(tableName, "UnitTest");
    table.addColumn("Test_Column1");
    table.addColumn("Test_Column2");

    return table;
  }

  public static void insertTestData(CTable ctable, int start, int end)
      throws IOException {
    insertTestData(ctable, start, end, 0);
  }

  public static void insertTestData(CTable ctable, int start, int end,
      int sleepTime) throws IOException {
    insertTestData(ctable, start, end, sleepTime, "row_");
  }

  public static void insertTestData(CTable ctable, int start, int end,
      int sleepTime, String prefix) throws IOException {
    String[] columns = ctable.descTable().getColumnsArray();

    if (columns.length != 2) {
      throw new IOException("Test table must have 2 columns");
    }

    long startTime = System.currentTimeMillis();

    // Write out a bunch of values

    for (int k = start; k <= end; k++) {
      // if(k % 100 == 0) {
      // System.out.println("Write row_" + df.format(k));
      // }
      Row.Key rowKey = new Row.Key(prefix + df.format(k));

      long insertStartTime = System.currentTimeMillis();
      while (true) {
        // startTx가 정상적으로 수행된 상태에서 insert, commit가 되지 않는 것은 그 동안 장애가 발생한 경우이다.
        // 따라서 다시 startTx 부터 시작하도록 해야 한다.
        Row row = new Row(rowKey);
        try {
          row.addCell(columns[0],new Cell(new Cell.Key(CONTENTS_KEY), (CONTENTSTR + df.format(k)).getBytes()));
          row.addCell(columns[1],new Cell(new Cell.Key(ANCHOR_KEY + df.format(k)), (ANCHORSTR + df.format(k)).getBytes()));
          ctable.put(row);
          break;
        } catch (IOException e) {
        }
        if (System.currentTimeMillis() - insertStartTime > 120 * 1000) {
          throw new IOException("Timeout while insert:" + rowKey);
        }
      }
      if (sleepTime > 0) {
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
          return;
        }
      }
    }
    System.out.println("Write " + (end - start + 1) + " rows. Elapsed time: "
        + ((System.currentTimeMillis() - startTime) / 1000.0));
  }

  public static Row.Key getAndAssert(CTable ctable, int rowKeyIndex,
      int matchIndex, String[] columns) throws IOException {
    return getAndAssert(ctable, rowKeyIndex, matchIndex, columns, true);
  }

  public static Row.Key getAndAssert(CTable ctable, int rowKeyIndex,
      int matchIndex, String[] columns, boolean exists) throws IOException {
    Row.Key rowKey = new Row.Key("row_" + df.format(rowKeyIndex));
    // System.out.println("Get:" + rowKey);
    Row row = ctable.get(rowKey, columns);
    if (exists) {
      TestCase.assertNotNull("result of get operation must be not null",
          row);
      TestCase.assertEquals("result of get operation has " + columns.length
          + " column record", columns.length, row.getColumnCount());
      for (int i = 0; i < columns.length; i++) {
        List<Cell> cells = row.getCellList(columns[i]);
        TestCase.assertNotNull("result for " + columns[i] + " is not null("
            + rowKey + ")", cells);
        TestCase
            .assertEquals("result has one item.", 1, cells.size());
        TestCase.assertNotNull(cells.get(0));
      }
    } else {
      for (int i = 0; i < columns.length; i++) {
        List<Cell> cells = row.getCellList(columns[i]);
        TestCase.assertNull(cells);
      }
    }
    return rowKey;
  }

  public void setUp() throws Exception {
    TestHandler.clear();
    miniCommitLogCluster = MiniCommitLogCluster.create(conf);
  }

  public void tearDown() throws Exception {
    miniCommitLogCluster.shutdown();
    TestHandler.clear();
    TabletLocationCache.getInstance(conf).clearAllCache();
  }
}
