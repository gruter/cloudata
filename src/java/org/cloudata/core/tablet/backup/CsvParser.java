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
package org.cloudata.core.tablet.backup;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class CsvParser {
  StringBuffer strbuf = new StringBuffer();

  List<String> list = new ArrayList<String>();

  public String[] parse(String line) throws IOException {
    while ((line != null)
        && (line.startsWith("#") || (line.trim().length() == 0))) {
      return null;
    }
    int ptr = 0;
    boolean inQuote = false;
    list.clear();
    strbuf.setLength(0);
    if (line != null) {
      while (ptr < line.length()) {
        char ch = line.charAt(ptr);
        if (ch == '"') {
          if ((ptr < line.length() - 1) && (line.charAt(ptr + 1) == '"')) {
            strbuf.append("\"");
            ptr += 2;
          } else if (inQuote) {
            inQuote = false;
            ptr++;
          } else {
            inQuote = true;
            ptr++;
          }
        } else if (ch == ',') {
          if (inQuote) {
            strbuf.append(ch);
            ptr++;
          } else {
            list.add(strbuf.toString());
            strbuf.setLength(0);
            ptr++;
          }
        } else {
          strbuf.append(ch);
          ptr++;
        }
      }
      list.add(strbuf.toString());
      String[] array = new String[list.size()];
      array = (String[]) list.toArray(array);
      return array;
    } else {
      return null;
    }
  }
}