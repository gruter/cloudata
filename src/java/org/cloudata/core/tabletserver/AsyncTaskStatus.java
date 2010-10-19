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
package org.cloudata.core.tabletserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.cloudata.core.common.io.CWritable;
import org.cloudata.core.common.io.CWritableUtils;


public class AsyncTaskStatus implements CWritable {
  protected boolean error;
  protected String errorTrace = "";
  protected boolean end;
  
  public AsyncTaskStatus() {
    errorTrace = "";
  }
  public boolean isEnd() {
    return end;
  }

  public boolean isError() {
    return error;
  }

  public String getErrorTrace() {
    return errorTrace;
  }

  public String toString() {
    return "end=" + end + ",error=" + error + ",errorTrace=" + errorTrace;
  }
  
  public void readFields(DataInput in) throws IOException {
    error =  in.readBoolean();
    errorTrace = CWritableUtils.readString(in);
    end = in.readBoolean();
  }

  public void write(DataOutput out) throws IOException {
    out.writeBoolean(error);
    CWritableUtils.writeString(out, errorTrace);
    out.writeBoolean(end);
  }  
}
