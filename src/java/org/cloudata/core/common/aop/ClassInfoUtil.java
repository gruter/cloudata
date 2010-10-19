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
package org.cloudata.core.common.aop;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class ClassInfoUtil {
  private static ClassInfoUtil instance;

  private HashMap<String, ClassInfo> classInfos;

  private ClassInfoUtil() {
    classInfos = new HashMap<String, ClassInfo>();
    loadClassInfos();
  }

  public void setInvokeHandler(String classId, List<String> handlers) {
    ClassInfo classInfo = classInfos.get(classId);
    if(classInfo != null) {
      classInfo.setHandlers(handlers);
    }
  }
  
  private void loadClassInfos() {
    loadClassInfosFrom("classInfo.xml");
    loadClassInfosFrom("classInfo-site.xml");
  }

  private void loadClassInfosFrom(String fileName) {
	try {
      InputStream in = getClass().getClassLoader().getResourceAsStream(fileName);
      
      if (in == null) {
    	  return;
      }
      
      DocumentBuilder documentBuilder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document document = documentBuilder.parse(in);

      NodeList classNodeList = document.getElementsByTagName("class");
      int nodeListLength = classNodeList.getLength();

      for (int i = 0; i < nodeListLength; i++) {
        Element classElement = (Element) classNodeList.item(i);

        String classId = classElement.getAttributes().getNamedItem("id").getNodeValue();
        String classType = classElement.getAttributes().getNamedItem("type").getNodeValue();
        List<String> handlers = getHandlers(classElement);
        classInfos.put(classId, new ClassInfo(classId, classType, handlers));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private List<String> getHandlers(Element classElement) throws Exception {
    List<String> result = new ArrayList<String>();
    NodeList managerNodes = classElement.getElementsByTagName("handler");
    for (int j = 0; j < managerNodes.getLength(); j++) {
      Element managerNode = (Element) managerNodes.item(j);
      result.add(managerNode.getAttributes().getNamedItem("type").getNodeValue());
    }

    return result;
  }

  public static ClassInfoUtil getInstance() {
    if (instance == null) {
      instance = new ClassInfoUtil();
    }
    return instance;
  }

  public ClassInfo getClassInfo(String classId) throws Exception {
    if (classInfos == null) {
      throw new Exception("no class info. check classinfo.xml");
    }

    try {
      ClassInfo classInfo = (ClassInfo) classInfos.get(classId);

      if (classInfo == null)
        throw new Exception("no class info[" + classId + "]. check classinfo.xml");

      return classInfo;
    } catch (Exception e) {
      throw e;
    }
  }

  public static void clearClassInfo() {
    instance = null;
  }
}
