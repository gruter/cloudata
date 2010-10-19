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
package org.cloudata.core;

import java.io.IOException;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.log4j.Logger;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.DirectUploader;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.ScanCell;
import org.cloudata.core.client.scanner.ScannerFactory;
import org.cloudata.core.client.scanner.TableScanner;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;


/**
 * @author jindolk
 *
 */
public class PerformanceTest {
  static final Logger LOG =
    Logger.getLogger(PerformanceTest.class.getName());
  
  private static final int ROW_LENGTH = 1000;
  private static final int ONE_GB = 1024 * 1024 * 1000;
  private static final int ROWS_PER_GB = ONE_GB / ROW_LENGTH;
  
  private static final String COLUMN_NAME = "info";
  private static final String COLUMN_KEY = "data";
  protected static TableSchema tableInfo;
  static {
    tableInfo = new TableSchema("PerformanceTest");
    tableInfo.addColumn(COLUMN_NAME);
  }
  
  private static final String RANDOM_READ = "randomRead";
  private static final String RANDOM_READ_MEM = "randomReadMem";
  private static final String RANDOM_WRITE = "randomWrite";
  private static final String SEQUENTIAL_READ = "sequentialRead";
  private static final String SEQUENTIAL_WRITE = "sequentialWrite";
  private static final String SCAN = "scan";
  private static final String BATCH_UPLOAD = "batchUpload";
  
  private static final List<String> COMMANDS =
    Arrays.asList(new String [] {RANDOM_READ,
      RANDOM_READ_MEM,
      RANDOM_WRITE,
      SEQUENTIAL_READ,
      SEQUENTIAL_WRITE,
      SCAN,
      BATCH_UPLOAD});
  
  volatile Configuration conf;
  private boolean miniCluster = false;
  private int N = 1;
  private int R = ROWS_PER_GB;
  private int tabletCount = 5;
  private boolean batchUpload = false;
  
  private SortedSet<Row.Key> rowKeySet = new TreeSet<Row.Key>();
  private static final Path PERF_EVAL_DIR = new Path("performance_test");
  private CloudataConf cConf = new CloudataConf();
  
  /**
   * Regex to parse lines in input file passed to mapreduce task.
   */
  public static final Pattern LINE_PATTERN =
    Pattern.compile("startRow=(\\d+),\\s+" +
    "perClientRunRows=(\\d+),\\s+totalRows=(\\d+),\\s+clients=(\\d+)");
  
  public static final DecimalFormat df = new DecimalFormat("0000000000");
  
  /**
   * Enum for map metrics.  Keep it out here rather than inside in the Map
   * inner-class so we can find associated properties.
   */
  protected static enum Counter {
    /** elapsed time */
    ELAPSED_TIME,
    /** number of rows */
    ROWS}
  
  
  /**
   * Constructor
   * @param c Configuration object
   */
  public PerformanceTest(final Configuration c) {
    this.conf = c;
  }
  
  /**
   * Implementations can have their status set.
   */
  static interface Status {
    /**
     * Sets status
     * @param msg status message
     * @throws IOException
     */
    void setStatus(final String msg) throws IOException;
  }
  
  /**
   * MapReduce job that runs a performance evaluation client in each map task.
   */
  @SuppressWarnings("unchecked")
  public static class EvaluationMapTask extends MapReduceBase
    implements Mapper<WritableComparable, Writable, LongWritable, Text> {
    /** configuration parameter name that contains the command */
    public final static String CMD_KEY = "EvaluationMapTask.command";
    private String cmd;
    private PerformanceTest pTest;
    
    /** {@inheritDoc} */
    @Override
    public void configure(JobConf j) {
      this.cmd = j.get(CMD_KEY);

      this.pTest = new PerformanceTest(j);
    }
    
    /** {@inheritDoc} */
    public void map(@SuppressWarnings("unused") final WritableComparable key,
      final Writable value, final OutputCollector<LongWritable, Text> output,
      final Reporter reporter)
    throws IOException {
      Matcher m = LINE_PATTERN.matcher(((Text)value).toString());
      if (m != null && m.matches()) {
        int startRow = Integer.parseInt(m.group(1));
        int perClientRunRows = Integer.parseInt(m.group(2));
        int totalRows = Integer.parseInt(m.group(3));
        Status status = new Status() {
          public void setStatus(String msg) {
            try {
              reporter.setStatus(msg);
            } catch (Exception e) {
            }
          }
        };
        long elapsedTime =  this.pTest.runOneClient(this.cmd, startRow,
          perClientRunRows, totalRows, status);
        // Collect how much time the thing took.  Report as map output and
        // to the ELAPSED_TIME counter.
        reporter.incrCounter(Counter.ELAPSED_TIME, elapsedTime);
        reporter.incrCounter(Counter.ROWS, perClientRunRows);
        output.collect(new LongWritable(startRow),
          new Text(Long.toString(elapsedTime)));
      }
    }
  }
  
  /*
   * If table does not already exist, create.
   * @param c Client to use checking.
   * @return True if we created the table.
   * @throws IOException
   */
  private boolean checkTable() throws IOException {
    if(batchUpload) {
      int rowPerTablet = this.R/tabletCount;
      
      SortedSet<Row.Key> sortedRow = new TreeSet<Row.Key>(); 
      for(int i = 0; i < this.R; i++) {
        String rowKeyVal = df.format(i);
        sortedRow.add(new Row.Key(rowKeyVal));
      }
      
      int index = 1;
      for(Row.Key eachRowKey: sortedRow) {
        if(index != 1 && index % rowPerTablet == 0) {
          rowKeySet.add(eachRowKey);
          //System.out.println("RowKey Range:" + eachHashRowKey);
        }
        index++;
      }
      rowKeySet.add(Row.Key.MAX_KEY);
      //System.out.println("RowKey Range:" + HashRowKey.maxRowKey);
      CTable.createTable(cConf, tableInfo, rowKeySet.toArray(new Row.Key[rowKeySet.size()]));
    } else {
      if(!CTable.existsTable(cConf, tableInfo.getTableName())) {
        CTable.createTable(cConf, tableInfo);
        LOG.info("Table " + tableInfo + " created");
      } else {
        LOG.info("Table " + tableInfo + " exists. use existed table");
      }
    }
    
    try {
      Thread.sleep(5*1000);
    } catch (InterruptedException e) {
    }
    return true;
  }
 
  /*
   * We're to run multiple clients concurrently.  Setup a mapreduce job.  Run
   * one map per client.  Then run a single reduce to sum the elapsed times.
   * @param cmd Command to run.
   * @throws IOException
   */
  private void runNIsMoreThanOne(final String cmd)
  throws IOException {
    checkTable();
    
    // Run a mapreduce job.  Run as many maps as asked-for clients.
    // Before we start up the job, write out an input file with instruction
    // per client regards which row they are to start on.
    Path inputDir = writeInputFile(this.conf);
    this.conf.set(EvaluationMapTask.CMD_KEY, cmd);
    JobConf job = new JobConf(this.conf, this.getClass());
    FileInputFormat.addInputPath(job, inputDir);
    job.setInputFormat(TextInputFormat.class);
    job.setJobName("Cloudata Performance Evaluation");
    job.setMapperClass(EvaluationMapTask.class);
    job.setMaxMapAttempts(1);
    job.setMaxReduceAttempts(1);
    job.setNumMapTasks(this.N * 10); // Ten maps per client.
    job.setNumReduceTasks(1);
    job.setOutputFormat(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(inputDir, "outputs"));
    JobClient.runJob(job);
  }
  
  /*
   * Write input file of offsets-per-client for the mapreduce job.
   * @param c Configuration
   * @return Directory that contains file written.
   * @throws IOException
   */
  private Path writeInputFile(final Configuration c) throws IOException {
    FileSystem fs = FileSystem.get(c);
    if (!fs.exists(PERF_EVAL_DIR)) {
      fs.mkdirs(PERF_EVAL_DIR);
    }
    SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
    Path subdir = new Path(PERF_EVAL_DIR, formatter.format(new Date()));
    fs.mkdirs(subdir);
    Path inputFile = new Path(subdir, "input.txt");
    PrintStream out = new PrintStream(fs.create(inputFile));
    try {
      for (int i = 0; i < (this.N * 10); i++) {
        // Write out start row, total number of rows per client run: 1/10th of
        // (R/N).
        int perClientRows = (this.R / this.N);
        out.println("startRow=" + i * perClientRows +
          ", perClientRunRows=" + (perClientRows / 10) +
          ", totalRows=" + this.R +
          ", clients=" + this.N);
      }
    } finally {
      out.close();
    }
    return subdir;
  }
  
  /*
   * A test.
   * Subclass to particularize what happens per row.
   */
  static abstract class Test {
    protected final Random rand = new Random(System.currentTimeMillis());
    protected final int startRow;
    protected final int perClientRunRows;
    protected final int totalRows;
    private final Status status;
    protected CTable table;
    protected volatile Configuration conf;
    
    Test(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super();
      this.startRow = startRow;
      this.perClientRunRows = perClientRunRows;
      this.totalRows = totalRows;
      this.status = status;
      this.table = null;
      this.conf = conf;
    }
    
    /*
     * @return Generated random value to insert into a table cell.
     */
    byte[] generateValue() {
      byte [] b = new byte [(int)ROW_LENGTH];
      rand.nextBytes(b);
      return b;
    }
    
    private String generateStatus(final int sr, final int i, final int lr) {
      return sr + "/" + i + "/" + lr;
    }
    
    protected int getReportingPeriod() {
      return this.perClientRunRows / 100;
    }
    
    void testSetup() throws IOException {
      this.table = CTable.openTable(new CloudataConf(), tableInfo.getTableName());
//      this.table.setAutoFulsh(false);
//      this.table.setBufferSize(12 * 1024 * 1024);
    }

    @SuppressWarnings("unused")
    void testTakedown()  throws IOException {
      // Empty
      //this.table.flush();
    }
    
    /*
     * Run test
     * @return Elapsed time.
     * @throws IOException
     */
    long test() throws IOException {
      long elapsedTime;
      testSetup();
      long startTime = System.currentTimeMillis();
      try {
        int lastRow = this.startRow + this.perClientRunRows;
        // Report on completion of 1/10th of total.
        for (int i = this.startRow; i < lastRow; i++) {
          testRow(i);
          if (status != null && i > 0 && (i % getReportingPeriod()) == 0) {
            status.setStatus(generateStatus(this.startRow, i, lastRow));
          }
        }
        elapsedTime = System.currentTimeMillis() - startTime;
      } finally {
        testTakedown();
      }
      return elapsedTime;
    }
    
    String getRandomRow() {
      return df.format(rand.nextInt(Integer.MAX_VALUE) % this.totalRows);
    }
    
    /*
     * Test for individual row.
     * @param i Row index.
     */
    abstract void testRow(final int i) throws IOException;
    
    /*
     * @return Test name.
     */
    abstract String getTestName();
  }
  
  class RandomReadTest extends Test {
    RandomReadTest(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(@SuppressWarnings("unused") final int i) throws IOException {
      Row.Key rowKey = new Row.Key(getRandomRow());
//      System.out.println("Get:" + rowKey);
      byte[] data = this.table.get(rowKey, COLUMN_NAME, new Cell.Key(COLUMN_KEY));
      if(data == null) {
        LOG.info("result is null");
      }
    }

    @Override
    protected int getReportingPeriod() {
      // 
      return this.perClientRunRows / 100;
    }

    @Override
    String getTestName() {
      return "randomRead";
    }
  }
  
  class RandomWriteTest extends Test {
    RandomWriteTest(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(@SuppressWarnings("unused") final int i) throws IOException {
      Row.Key rowKey = new Row.Key(getRandomRow());
      try {
        Row row = new Row(rowKey);
        row.addCell(COLUMN_NAME, new Cell(new Cell.Key(COLUMN_KEY), generateValue()));
        table.put(row);
      } catch (IOException e) {
        LOG.error("RandowWrite testRow error:" + e.getMessage(), e);
      }
    }

    @Override
    String getTestName() {
      return "randomWrite";
    }
  }
  
  class ScanTest extends Test {
    private TableScanner testScanner;
    
    int count = 0;
    ScanTest(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testSetup() throws IOException {
      super.testSetup();
      this.testScanner = ScannerFactory.openScanner(table, COLUMN_NAME);
    }
    
    @Override
    void testTakedown() throws IOException {
      if (this.testScanner != null) {
        this.testScanner.close();
      }
      super.testTakedown();
    }
    
    
    @Override
    void testRow(@SuppressWarnings("unused") final int i) throws IOException {
      ScanCell columnValue = this.testScanner.next();
      if(columnValue == null) {
        System.out.println("Scan result is null");
      } else {
        //System.out.println(count + ">" + columnValue);
        count++;
      }
    }

    @Override
    String getTestName() {
      return "scan";
    }
  }
  
  class SequentialReadTest extends Test {
    SequentialReadTest(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int i) throws IOException {
      byte[] data = table.get(new Row.Key(df.format(i)), COLUMN_NAME, new Cell.Key(COLUMN_KEY));
      if(data == null) {
        System.out.println(df.format(i) + " result is null");
      }
    }

    @Override
    String getTestName() {
      return "sequentialRead";
    }
  }
  
  class BatchUploadTest extends Test {
    DirectUploader batchUploaders;
    BatchUploadTest(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int row) throws IOException {
      if(batchUploaders == null) {
        CTable ctable = CTable.openTable(cConf, tableInfo.getTableName());
        batchUploaders = ctable.openDirectUploader(ctable.getColumnsArray());
      }
      Row.Key rowKey = new Row.Key(df.format(row));
      Row rowData = new Row(rowKey);
      rowData.addCell(COLUMN_NAME, new Cell(new Cell.Key(COLUMN_KEY), generateValue()));
      batchUploaders.put(rowData);
    }

    @Override
    String getTestName() {
      return "batchUpload";
    }
    
    void testTakedown()  throws IOException {
      LOG.info("BatchUploader.close()");
      batchUploaders.close();
    }
  }
  
  class SequentialWriteTest extends Test {
    //BatchUploader[] batchUploaders;
    SequentialWriteTest(final Configuration conf, final int startRow,
        final int perClientRunRows, final int totalRows, final Status status) {
      super(conf, startRow, perClientRunRows, totalRows, status);
    }
    
    @Override
    void testRow(final int row) throws IOException {
      try {
        Row.Key rowKey = new Row.Key(df.format(row));
        Row rowData = new Row(rowKey);
        rowData.addCell(COLUMN_NAME, new Cell(new Cell.Key(COLUMN_KEY), generateValue()));
        table.put(rowData);
      } catch (Exception e) {
        System.out.println("Insert Error:" + e.getMessage());
      }
    }

    @Override
    String getTestName() {
      return "sequentialWrite";
    }
  }
  
  long runOneClient(final String cmd, final int startRow,
    final int perClientRunRows, final int totalRows, final Status status)
  throws IOException {
    status.setStatus("Start " + cmd + " at offset " + startRow + " for " +
      perClientRunRows + " rows");
    long totalElapsedTime = 0;
    if (cmd.equals(RANDOM_READ)) {
      Test t = new RandomReadTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(RANDOM_READ_MEM)) {
      throw new UnsupportedOperationException("Not yet implemented");
    } else if (cmd.equals(RANDOM_WRITE)) {
      Test t = new RandomWriteTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SCAN)) {
      Test t = new ScanTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SEQUENTIAL_READ)) {
      Test t = new SequentialReadTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(SEQUENTIAL_WRITE)) {
      Test t = new SequentialWriteTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else if (cmd.equals(BATCH_UPLOAD)) {
      Test t = new BatchUploadTest(this.conf, startRow, perClientRunRows,
        totalRows, status);
      totalElapsedTime = t.test();
    } else {
      new IllegalArgumentException("Invalid command value: " + cmd);
    }
    status.setStatus("Finished " + cmd + " in " + totalElapsedTime +
      "ms at offset " + startRow + " for " + perClientRunRows + " rows");
    System.out.println("Finished " + cmd + " in " + totalElapsedTime +
      "ms at offset " + startRow + " for " + perClientRunRows + " rows");
    return totalElapsedTime;
  }
  
  private void runNIsOne(final String cmd) throws IOException {
    Status status = new Status() {
      @SuppressWarnings("unused")
      public void setStatus(String msg) throws IOException {
        LOG.info(msg);
      }
    };

    try {
      if(cmd.equals(BATCH_UPLOAD)) {
        batchUpload = true;
      }
      
      boolean needInsert = true;
      if(CTable.existsTable(cConf, tableInfo.getTableName())) {
        needInsert = false;
      }
      checkTable();

      if (cmd.equals(RANDOM_READ) || cmd.equals(RANDOM_READ_MEM) ||
          cmd.equals(SCAN) || cmd.equals(SEQUENTIAL_READ)) {
        if(needInsert) {
          status.setStatus("Running " + SEQUENTIAL_WRITE + " first so " +
              cmd + " has data to work against");
          runOneClient(SEQUENTIAL_WRITE, 0, this.R, this.R, status);
        }
      }
      
      runOneClient(cmd, 0, this.R, this.R, status);
    } catch (Exception e) {
      LOG.error("Failed", e);
    } finally {
      //LOG.info("Deleting table " + tableInfo.getTableName());
      //NTable.dropTable(tableInfo.getTableName());
    }
  }
  
  private void runTest(final String cmd) throws IOException {
    if (cmd.equals(RANDOM_READ_MEM)) {
      // For this one test, so all fits in memory, make R smaller (See
      // pg. 9 of BigTable paper).
      R = (ONE_GB / 10) * N;
    }
    
    try {
      if (N == 1) {
        // If there is only one client and one HRegionServer, we assume nothing
        // has been set up at all.
        runNIsOne(cmd);
      } else {
        // Else, run 
        runNIsMoreThanOne(cmd);
      }
    } finally {
      
    }
  }
  
  private void printUsage() {
    printUsage(null);
  }
  
  private void printUsage(final String message) {
    if (message != null && message.length() > 0) {
      System.err.println(message);
    }
    System.err.println("Usage: java " + this.getClass().getName() + "<command> <nclients>");
    System.err.println();
    System.err.println("Command:");
    System.err.println(" randomRead      Run random read test");
    System.err.println(" randomReadMem   Run random read test where table " +
      "is in memory");
    System.err.println(" randomWrite     Run random write test");
    System.err.println(" sequentialRead  Run sequential read test");
    System.err.println(" sequentialWrite Run sequential write test");
    System.err.println(" scan            Run scan test");
    System.err.println(" batchUpload     Run batchUpload test");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" nclients        Integer. Required. Total number of client");
    System.err.println("                 running: 1 <= value <= 500");
    System.err.println("Examples:");
    System.err.println(" To run a single evaluation client:");
    System.err.println(" $ bin/cloudata jar cloudata-1.0-dev-test.jasr " +
      "org.cloudata.core.PerformanceTest sequentialWrite 1");
  }

  private void getArgs(final int start, final String[] args) {
    if(start + 1 > args.length) {
      throw new IllegalArgumentException("must supply the number of clients");
    }
    
    N = Integer.parseInt(args[start]);
    if (N > 500 || N < 1) {
      throw new IllegalArgumentException("Number of clients must be between " +
        "1 and 500.");
    }
   
    // Set total number of rows to write.
    R = ROWS_PER_GB * N;
  }
  
  public int doCommandLine(final String[] args) {
    // Process command-line args. TODO: Better cmd-line processing
    // (but hopefully something not as painful as cli options).    
    int errCode = -1;
    if (args.length < 1) {
      printUsage();
      return errCode;
    }
    
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage();
          errCode = 0;
          break;
        }
        
//        final String masterArgKey = "--master=";
//        if (cmd.startsWith(masterArgKey)) {
//          this.conf.set(MASTER_ADDRESS, cmd.substring(masterArgKey.length()));
//          continue;
//        }
//       
//        final String miniClusterArgKey = "--miniCluster";
//        if (cmd.startsWith(miniClusterArgKey)) {
//          this.miniCluster = true;
//          continue;
//        }
       
        if (COMMANDS.contains(cmd)) {
          getArgs(i + 1, args);
          runTest(cmd);
          errCode = 0;
          break;
        }
    
        printUsage();
        break;
      }
    } catch (Exception e) {
      System.out.println(e.getMessage());
      //e.printStackTrace();
    }
    
    return errCode;
  }
  
  /**
   * @param args
   */
  public static void main(String[] args) throws IOException {
    System.exit(new PerformanceTest(new Configuration()).
      doCommandLine(args));
  }
}
