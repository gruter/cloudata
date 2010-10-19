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
package org.cloudata.examples.weblink;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.parallel.hadoop.CloudataMapReduceUtil;
import org.cloudata.core.tablet.TableSchema;
import org.htmlparser.Node;
import org.htmlparser.Parser;
import org.htmlparser.tags.LinkTag;
import org.htmlparser.tags.TitleTag;
import org.htmlparser.util.NodeIterator;
import org.htmlparser.util.NodeList;
import org.htmlparser.util.ParserException;


public class UploadJob {
  public static void main(String[] args) throws IOException {
     (new UploadJob()).run(args);
  }
  
  public void run(String[] args) throws IOException {
    if (args.length < 3) {
      System.out.println("Usage: java UploadJob <input path> <table name> <distributed cache dir>");
      System.exit(0);
    }

    Path inputPath = new Path(args[0]);
    String tableName = args[1];

    CloudataConf nconf = new CloudataConf();
    if (!CTable.existsTable(nconf, tableName)) {
      TableSchema tableSchema = new TableSchema(tableName);
      tableSchema.addColumn("url");
      tableSchema.addColumn("page");
      tableSchema.addColumn("title");
      tableSchema.addColumn("outlink");

      CTable.createTable(nconf, tableSchema);
    }

    JobConf jobConf = new JobConf(UploadJob.class);
    jobConf.set("mapred.child.java.opts", "-Xss4096K");
    jobConf.setJobName("CloudataExamles.weblink.UploadJob_" + new Date());
    String libDir = CloudataMapReduceUtil.initMapReduce(jobConf);

    DistributedCache.addArchiveToClassPath(new Path(args[2] + "/htmllexer.jar"), jobConf);
    DistributedCache.addArchiveToClassPath(new Path(args[2] + "/htmlparser.jar"), jobConf);
    DistributedCache.addArchiveToClassPath(new Path(args[2] + "/jdom.jar"), jobConf);

    // <MAP>
    FileInputFormat.addInputPath(jobConf, inputPath);
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.setMapperClass(UploadJobMapper.class);
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, tableName);
    // </MAP>

    // <REDUCE>
    // Map Only
    FileOutputFormat.setOutputPath(jobConf, new Path("CloudataExamles_WebUploadJob_" + System.currentTimeMillis()));
    jobConf.setNumReduceTasks(0);
    // </REDUCE>

    try {
      JobClient.runJob(jobConf);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      FileSystem fs = FileSystem.get(jobConf);
      fs.delete(FileOutputFormat.getOutputPath(jobConf), true);
      CloudataMapReduceUtil.clearMapReduce(libDir);
    }
  }

  static class UploadJobMapper implements
      Mapper<LongWritable, Text, Text, Text> {
    /*
     * ==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==> URL:
     * http://www.adobe.com/robots.txt Date: Position: 0 DocId: 0
     *
     * HTTP/1.1 200 OK Date: Wed, 28 Feb 2007 17:28:10 GMT Server: Apache
     * Cache-Control: max-age=21600 Expires: Wed, 28 Feb 2007 23:28:10 GMT
     * Content-Type: text/html Connection: close
     *
     * contents....
     */

    static Log LOG = LogFactory.getLog(UploadJobMapper.class.getName());

    static String DELIM = "==P=>>>>=i===<<<<=T===>=A===<=!Junghoo!==>";

    private StringBuilder sb = new StringBuilder();

    private int emptyLineCount;

    private String url;

    private String title;

    private List<LinkInfo> linkInfos = new ArrayList<LinkInfo>();

    private CTable ctable;

    private IOException err;

    private boolean startPage = false;

    private int count = 0;
    
    private long pageSizeSum = 0;

    private void clear() {
      startPage = true;
      sb.setLength(0);
      url = null;
      emptyLineCount = 0;
      linkInfos.clear();
    }

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException {
      if(err != null) {
        throw err;
      }
      String line = value.toString();
      if (DELIM.equals(line)) {
        if (sb.length() > 0) {
          if(sb.length() > 5 * 1024 * 1024) {
            System.out.println("HTML too big:" + sb.length());
            clear();
            return;
          }
          pageSizeSum += sb.toString().getBytes().length;
          insert(reporter);
        }
        clear();
        return;
      }

      if (!startPage) {
        return;
      }

      if (line == null || line.trim().length() == 0) {
        if (emptyLineCount <= 1) {
          emptyLineCount++;
          return;
        }
      }

      // header
      if (emptyLineCount <= 1) {
        if (line.startsWith("URL:")) {
          url = line.substring(5).trim();
        }
      } else {
        if(sb.length() > 5 * 1024 * 1024) {
          System.out.println("HTML too bing:" + sb.length());
        } else {
          sb.append(line).append("\n");
        }
      }
      if(reporter != null) {
        reporter.progress();
      }
    }

    private void insert(Reporter reporter) throws IOException {
      Parser parser = null;
      try {
        parser = new Parser(sb.toString());

        for (NodeIterator e = parser.elements(); e.hasMoreNodes();) {
          Node node = e.nextNode();
          parseTag(node);
        }
      } catch (Exception e) {
        title = "no title";
        linkInfos = new ArrayList<LinkInfo>();
        LOG.warn("Parse Error:" + url);
      }

      if(reporter != null) {
        reporter.progress();
      }

      if(url == null) {
        return;
      }

      Row.Key rowKey = new Row.Key(String.valueOf(url.hashCode()));
      Row row = new Row(rowKey);
      row.addCell("url", new Cell(Cell.Key.EMPTY_KEY, url.getBytes()));
      row.addCell("page",
          new Cell(Cell.Key.EMPTY_KEY, sb.toString().getBytes()));
      if(title != null) {
        row.addCell("title", new Cell(Cell.Key.EMPTY_KEY, title.getBytes()));
      }
      int linkCount = 0;
      for (LinkInfo eachLinkInfo : linkInfos) {
        row.addCell("outlink", new Cell(new Cell.Key(eachLinkInfo.link),
            eachLinkInfo.linkText.getBytes()));
        linkCount++;
        if(linkCount > 1000) {
          System.out.println("To Many Links. added only 1000 links:" + url + "," + linkInfos.size());
          break;
        }
      }

      try {
        if (ctable != null) {
          ctable.put(row);
          linkInfos.clear();
        } else {
        }
      } catch (Exception e) {
        LOG.error(rowKey.toString() + ":" + e.getMessage(), e);
      }

      count++;
//      System.out.println(count + " inserted, " + url);
      if(count % 1000 == 0) {
        LOG.info(count + " inserted, " + url);
      }
    }

    private void parseTag(Node node) throws ParserException {
      if (node.getClass().equals(LinkTag.class)) {
        LinkTag tag = (LinkTag) node;
        String link = tag.extractLink();
        String linkText = tag.getLinkText();
        int position = tag.getStartPosition();
        linkInfos.add(new LinkInfo(link, linkText, position));
      } else if (node.getClass().equals(TitleTag.class)) {
        TitleTag titleTag = (TitleTag) node;
        this.title = titleTag.toPlainTextString();
      }
      NodeList nodeList = node.getChildren();
      if (nodeList == null) {
        return;
      }

      for (NodeIterator e = nodeList.elements(); e.hasMoreNodes();) {
        Node childNode = e.nextNode();
        parseTag(childNode);
      }
    }

    @Override
    public void configure(JobConf job) {
      try {
        String tableName = job.get(AbstractTabletInputFormat.OUTPUT_TABLE);
        CloudataConf nconf = new CloudataConf(job);
        ctable = CTable.openTable(nconf, tableName);
        if(ctable == null) {
          throw new IOException("No table:" + tableName);
        }
      } catch (Exception e) {
        err = new IOException(e.getMessage());
        err.initCause(e);
      }
    }

    @Override
    public void close() throws IOException {
      LOG.info(count + " pages inserted, avg page length:" + (pageSizeSum/count));
    }
  }

  static class LinkInfo {
    String link;

    String linkText;

    int position;

    public LinkInfo(String link, String linkText, int position) {
      this.link = link;
      if(linkText == null) {
        linkText = "";
      }
      this.linkText = linkText;
      this.position = position;
    }
  }
}
