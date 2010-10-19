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
package org.cloudata.examples.web;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.common.util.CloudataLineReader;
import org.cloudata.core.parallel.hadoop.AbstractTabletInputFormat;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TabletInfo;


public class TermUploadJob {
  public static final Log LOG = LogFactory.getLog(TermUploadJob.class.getName()); 
  public static final String TERM_TABLE = "T_TERM";
  public static final String[] TERM_TABLE_COLUMNS = new String[]{"df", "tf", "weight", "i_weight"};
  
  public void exec(String[] options) throws Exception {
    if(options.length < 1) {
      System.out.println("Usage: java TermUploadJob <num of repeats> termUpload <inputPath> [#redcue]");
      System.exit(0);
    }
    JobConf jobConf = new JobConf(TermUploadJob.class);
    JobClient jobClinet = new JobClient(jobConf);
    int maxReduce = jobClinet.getClusterStatus().getMaxReduceTasks() * 2;
    if(options.length > 1) {
      maxReduce = Integer.parseInt(options[1]);
    }

    jobConf.setInt("mapred.task.timeout", 60 * 60 * 1000);
    
    FileSystem fs = FileSystem.get(jobConf);
    
    CloudataConf nconf = new CloudataConf();
    if(!CTable.existsTable(nconf, TERM_TABLE)) {
      //Table 파티션 작업
      Path path = new Path("blogdata/tmp/weight");
      FileStatus[] paths = fs.listStatus(path);
      if(paths == null || paths.length == 0) {
        LOG.error("No Partition info:" + path);
        return;
      }
      SortedSet<Text> terms = new TreeSet<Text>();
      Text text = new Text();
      for(FileStatus eachPath: paths) {
        CloudataLineReader reader = new CloudataLineReader(fs.open(eachPath.getPath()));
        while(true) {
          int length = reader.readLine(text);
          if(length <= 0) {
            break;
          }
          terms.add(new Text(text));
        }
      }
      
      int temrsPerTablet = terms.size() / (maxReduce - 1);
      int count = 0;
      List<Row.Key> rowKeys = new ArrayList<Row.Key>();
      for(Text term: terms) {
        count++;
        if(count == temrsPerTablet) {
          rowKeys.add(new Row.Key(term.getBytes()));
          count = 0;
        }
      }
      rowKeys.add(Row.Key.MAX_KEY);
      
      TableSchema temrTableInfo = new TableSchema(TERM_TABLE, "Test", TERM_TABLE_COLUMNS);
      CTable.createTable(nconf, temrTableInfo, rowKeys.toArray(new Row.Key[]{}));
    }
    CTable termTable = CTable.openTable(nconf, TERM_TABLE);
    TabletInfo[] tabletInfos = termTable.listTabletInfos();
    
    Path tempOutputPath = new Path("WebTableJob_" + System.currentTimeMillis());
    
    
    jobConf.setJobName("TermUploadJob" + "(" + new Date() + ")");
    FileInputFormat.addInputPath(jobConf, new Path(options[0]));
    
    //<MAP>
    jobConf.setMapperClass(TermUploadMap.class);
    jobConf.setMapOutputKeyClass(Text.class);
    jobConf.setMapOutputValueClass(Text.class);
    jobConf.setInputFormat(TextInputFormat.class);
    jobConf.set(AbstractTabletInputFormat.OUTPUT_TABLE, TERM_TABLE);
    jobConf.setPartitionerClass(WebKeyRangePartitioner.class);
    jobConf.setMaxMapAttempts(0);
    //</MAP>

    //<REDUCE>
    jobConf.setReducerClass(TermUploadReduce.class);
    jobConf.setOutputKeyClass(Text.class);
    jobConf.setOutputValueClass(Text.class);    
    jobConf.setNumReduceTasks(tabletInfos.length);
    FileOutputFormat.setOutputPath(jobConf, tempOutputPath);
    jobConf.setNumReduceTasks(maxReduce);
    jobConf.setMaxReduceAttempts(0);
    //<REDUCE>
    
    //Run Job
    JobClient.runJob(jobConf);
    
    fs.delete(tempOutputPath);
  }
}
