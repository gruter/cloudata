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
package org.cloudata.core.parallel.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class SimpleInputFormat implements InputFormat {
  public RecordReader getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
    return new SimpleRecordReader();
  }
  
  public void validateInput(JobConf job) throws IOException {
  }

  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    InputSplit[] result = new InputSplit[numSplits];
    Path outDir = FileOutputFormat.getOutputPath(job);
    for (int i = 0; i < result.length; ++i) {
      result[i] = new FileSplit(new Path(outDir, "part-" + i), 0, 1, (String[])null);
    }
    return result;
  }

  static class SimpleRecordReader implements RecordReader<WritableComparable, Writable> {
    boolean end = false;

    public boolean next(WritableComparable key, Writable value) {
      if (!end) {
        end = true;
        return true;
      } else {
        return false;
      }
    }

    public WritableComparable createKey() {
      return new Text();
    }

    public Writable createValue() {
      return new Text();
    }

    public long getPos() {
      return 0;
    }

    public void close() {
    }

    public float getProgress() {
      return 0.0f;
    }
  }
}
