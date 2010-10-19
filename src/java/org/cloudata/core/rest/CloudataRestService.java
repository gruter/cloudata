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
package org.cloudata.core.rest;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.cloudata.core.client.Cell;
import org.cloudata.core.client.CellFilter;
import org.cloudata.core.client.CTable;
import org.cloudata.core.client.Row;
import org.cloudata.core.client.RowFilter;
import org.cloudata.core.common.conf.CloudataConf;
import org.cloudata.core.tablet.TableSchema;
import org.cloudata.core.tablet.TableSchema.ColumnInfo;
import org.cloudata.core.tabletserver.CommitLog;
import org.restlet.util.XmlWriter;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;


/**
 * @author doe-nam kim
 *
 */
public class CloudataRestService {
  private static final Log LOG = LogFactory.getLog(CloudataRestService.class.getName());
  
  private static CloudataConf nconf = new CloudataConf();

  private CTable ctable = null;

  private static XmlWriter getXmlWriter(StringWriter buffer) {
    XmlWriter xmlWriter = new XmlWriter(buffer);
    xmlWriter.setDataFormat(true);
    xmlWriter.setIndentStep(2);
    
    return xmlWriter;
  }
  
  public String getTables() throws IOException, SAXException {
    TableSchema[] tables = CTable.listTables(nconf);

    StringWriter buffer = new StringWriter();
    XmlWriter resultDoc = getXmlWriter(buffer);
    resultDoc.startDocument();
    resultDoc.startElement("cloudata");

    for (TableSchema table : tables) {
      makeTableSchemaXml(table, resultDoc);
    }

    resultDoc.endElement("cloudata");
    resultDoc.endDocument();
    resultDoc.flush();

    return buffer.toString();
  }

  //
  public void createTable(String tableName, String documentXML)
      throws Exception {
    TableSchema tableSchema = getTableSchemaFromXml(documentXML);
    
    if(!tableName.equals(tableSchema.getTableName())) {
      throw new IOException("Different table name:" + tableName + "," + tableSchema.getTableName());
    }
    if (CTable.existsTable(nconf, tableName)) {
      throw new IOException(tableName + " table already exist.");
    }
    
    CTable.createTable(nconf, tableSchema);
  }

  // 이건 REST가 직관적으로 단어만 보고 그 사용도를 알수 있어야 함에도 불구하고 그렇지 못함
  // 사용한다면 경로 수정이 필요함
  public String getTable(String tableName) throws IOException, SAXException {
    if (!CTable.existsTable(nconf, tableName)) {
      throw new IOException(tableName + " table doesn't exist.");
    }
    ctable = CTable.openTable(nconf, tableName);
    return getTableSchemaXml(ctable.descTable());
  }

  //
  public void deleteTable(String tableName) throws IOException, Exception {
    if (CTable.existsTable(nconf, tableName)) {
      CTable.dropTable(nconf, tableName);
    } else {
      throw new Exception(tableName + " table doesn't exist.");
    }
  }

  public void putData(String tableName, String documentXML) throws Exception {
    if (CTable.existsTable(nconf, tableName)) {
      ctable = CTable.openTable(nconf, tableName);
    } else {
      throw new Exception(tableName + " table doesn't exist.");
    }
    
    for(Row row: getRowsFromXml(documentXML)) {
      ctable.put(row);
    }
  }

  public static List<Row> getRowsFromXml(String documentXML) throws IOException {
    try {
      List<Row> resultRows = new ArrayList<Row>();
      
      InputSource is = new InputSource(new StringReader(documentXML));
      DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = db.parse(is);
  
      NodeList rows = doc.getElementsByTagName("row");
      if(rows == null || rows.getLength() == 0) {
        throw new IOException("No row tag");
      }
  
      for (int i = 0; i < rows.getLength(); i++) {
        Node childNode = rows.item(i);
        if (childNode.getNodeType() == Node.TEXT_NODE) {
          continue;
        }
        Row row = getRowFromNode(childNode);
        resultRows.add(row);
      }
      return resultRows;
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }    
  }
  
  private static Row getRowFromNode(Node rowNode) throws IOException {
    try {
      String rowKey = rowNode.getAttributes().getNamedItem("key").getNodeValue();
      Row row = new Row(new Row.Key(rowKey));
      
      NodeList rowChilds = rowNode.getChildNodes();
      if(rowChilds == null || rowChilds.getLength() == 0) {
        throw new IOException("No column tag");
      }
      
      for (int i = 0; i < rowChilds.getLength(); i++) {
        Node childNode = rowChilds.item(i);
        if("column".equals(childNode.getNodeName())) {
          List<Cell> cells = getCellsFromNode(childNode);
          if(cells != null) {
            row.addCellList(childNode.getAttributes().getNamedItem("name").getNodeValue(), cells);
          }
        }
      }
      
      return row;
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }
  
  private static List<Cell> getCellsFromNode(Node columnNode) throws IOException {
    try {
      NodeList columnChilds = columnNode.getChildNodes();
      if(columnChilds == null || columnChilds.getLength() == 0) {
        return null;
      }
      
      List<Cell> cells = new ArrayList<Cell>();
      
      for (int i = 0; i < columnChilds.getLength(); i++) {
        Node childNode = columnChilds.item(i);
        if("cell".equals(childNode.getNodeName())) {
          String cellKey = childNode.getAttributes().getNamedItem("key").getNodeValue();
          
          Cell cell = new Cell(new Cell.Key(cellKey));
          
          NodeList chldNodeList = childNode.getChildNodes();
          for(int j = 0; j < chldNodeList.getLength(); j++) {
            Node valueNode = chldNodeList.item(j);
            if("value".equals(valueNode.getNodeName())) {
              long timestamp = Long.parseLong(valueNode.getAttributes().getNamedItem("timestamp").getNodeValue());
              if(timestamp == 0) {
                timestamp = CommitLog.USE_SERVER_TIMESTAMP;
              }
              
              String data = valueNode.getTextContent();
              
              cell.addValue(new Cell.Value(data.getBytes("UTF-8"), timestamp));
            }
          }
          
          cells.add(cell);
        }
      }
      return cells;
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }

  public void removeData(String tableName, String rowKey) throws IOException {
    ctable = CTable.openTable(nconf, tableName);
    ctable.removeRow(new Row.Key(rowKey));
  }

  private static void makeRowXml(Row row, XmlWriter resultDoc)  throws SAXException {
    AttributesImpl attr = new AttributesImpl();
    attr.addAttribute("", "key", "", "", row.getKey().toString());
    resultDoc.startElement("", "row", "", attr);
    attr.removeAttribute(0);

    String[] columns = row.getColumnNames();
    for (int i = 0; i < columns.length; i++) {
      makeColumnXml(row, columns[i], resultDoc);
    }
    resultDoc.endElement("row");
  }
  
  private static void makeColumnXml(Row row, String columnName, XmlWriter resultDoc) throws SAXException {
    List<Cell> columnCells = row.getCellList(columnName);
    
    AttributesImpl attr = new AttributesImpl();
    
    attr.addAttribute("", "name", "", "", columnName);
    resultDoc.startElement("", "column", "", attr);
    attr.removeAttribute(0);

    for (Cell cell : columnCells) {
      attr.addAttribute("", "key", "", "", cell.getKey().toString());
      resultDoc.startElement("", "cell", "", attr);
      attr.removeAttribute(0);

      attr.addAttribute("", "timestamp", "", "", Long.toString(cell
          .getValue().getTimestamp()));
      resultDoc.dataElement("", "value", "", attr, cell.getValue()
          .getValueAsString());
      attr.removeAttribute(0);
      /*
       * without timeStamp resultDoc.dataElement("value",
       * cell.getValue().getValueAsString()); resultDoc.endElement("cell");
       */
      resultDoc.endElement("cell");
    }
    resultDoc.endElement("column");
  }
  
  public String getData(String tableName, String previousRowKey , int pageSize) throws IOException,
  SAXException {
    ctable = CTable.openTable(nconf, tableName);

    RowFilter rowFilter = new RowFilter(Row.Key.MIN_KEY, Row.Key.MAX_KEY, RowFilter.OP_BETWEEN);
    rowFilter.addAllCellFilter(ctable.getTableSchema());
    rowFilter.setPagingInfo(pageSize, previousRowKey == null ? null : new Row.Key(previousRowKey));

    Row[] rows = ctable.gets(rowFilter);

    StringWriter buffer = new StringWriter();
    XmlWriter resultDoc = getXmlWriter(buffer);
    resultDoc.startDocument();
    resultDoc.startElement("cloudata");

    if(rows != null) {
      for(Row eachRow: rows) {
        makeRowXml(eachRow, resultDoc);
      }
    }
    resultDoc.endElement("cloudata");
    resultDoc.endDocument();
    resultDoc.flush();

    return buffer.toString();
  }
  
  public String getData(String tableName, String rowKey) throws IOException,
      SAXException {
    ctable = CTable.openTable(nconf, tableName);

    Row row = ctable.get(new Row.Key(rowKey));
    
    StringWriter buffer = new StringWriter();
    XmlWriter resultDoc = getXmlWriter(buffer);
    resultDoc.startDocument();
    resultDoc.startElement("cloudata");

    if(row != null) {
      makeRowXml(row, resultDoc);
    }
    
    resultDoc.endElement("cloudata");
    resultDoc.endDocument();
    resultDoc.flush();

    return buffer.toString();
  }

  public String getData(String tableName, String rowKey, String columnName)
      throws IOException, SAXException {
    ctable = CTable.openTable(nconf, tableName);

    StringWriter buffer = new StringWriter();
    XmlWriter resultDoc = getXmlWriter(buffer);

    resultDoc.startDocument();
    resultDoc.startElement("cloudata");

    Row row = ctable.get(new Row.Key(rowKey));
    if(row != null) {
      makeRowXml(row, resultDoc);
    }
    resultDoc.endElement("cloudata");
    resultDoc.endDocument();
    resultDoc.flush();

    return buffer.toString();
  }

  public String getData(String tableName, String rowKey, String columnName,
      String cellKey) throws SAXException, Exception {
    ctable = CTable.openTable(nconf, tableName);

    RowFilter rowFilter = new RowFilter(new Row.Key(rowKey));
    CellFilter cellFilter = new CellFilter(columnName, new Cell.Key(cellKey));
    cellFilter.setTimestamp(Long.MIN_VALUE, Long.MAX_VALUE);
    rowFilter.addCellFilter(cellFilter);

    Row row = ctable.get(rowFilter);

    StringWriter buffer = new StringWriter();
    XmlWriter resultDoc = getXmlWriter(buffer);
    resultDoc.startDocument();
    resultDoc.startElement("cloudata");

    if(row != null) {
      makeRowXml(row, resultDoc);
    }
    resultDoc.endElement("cloudata");
    resultDoc.endDocument();
    resultDoc.flush();

    return buffer.toString();
  }

  public String getErrorMessage(Throwable exception)  {
    StringWriter sw = new StringWriter();
    exception.printStackTrace(new PrintWriter(sw));
    
    return getResponseMessage(sw.toString(), true);
  }
  
  public String getResponseMessage(String message, boolean error) {
    try {
      StringWriter buffer = new StringWriter();
      XmlWriter resultDoc = getXmlWriter(buffer);
      AttributesImpl attr = new AttributesImpl();
      
      resultDoc.startDocument();
      resultDoc.startElement("cloudata");
      
      if(error) {
        attr.addAttribute("", "error", "", "", "yes");
        resultDoc.startElement("", "message", "", attr);
      } else {
        resultDoc.startElement("message");
      }
      resultDoc.characters(message);
      resultDoc.endElement("message");
      resultDoc.endElement("cloudata");
      resultDoc.endDocument();
      resultDoc.flush();
      
      return buffer.toString();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return e.getMessage();
    }
  }
  
  public static TableSchema getTableSchemaFromXml(String xml) throws IOException {
    /*
    <cloudata>
      <table name="T_BLOB_TEST01">
        <description></description>
        <column name="LOCAL_PATH" type="normal"></column>
        <column name="UPLOADED_FILE" type="blob"></column>
      </table>
    </cloudata>
     */
    try {
      TableSchema tableSchema = new TableSchema();
      
      InputSource is = new InputSource(new StringReader(xml));
      DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
      Document doc = db.parse(is);
  
      NodeList tables = doc.getElementsByTagName("table");
  
      if(tables == null || tables.getLength() == 0) {
        throw new IOException("No table tag");
      }
      
      Node tableNode = tables.item(0);
      
      String tableName = tableNode.getAttributes().getNamedItem("name").getNodeValue();
      tableSchema.setTableName(tableName);
      
      NodeList tableChildNodes = tableNode.getChildNodes();
      
      for(int i = 0; i < tableChildNodes.getLength(); i++) {
        Node childNode = tableChildNodes.item(i);
        
        if("description".equals(childNode.getNodeName())) {
          tableSchema.setDescription(childNode.getTextContent());
        } else if("column".equals(childNode.getNodeName())) {
          String columnName = childNode.getAttributes().getNamedItem("name").getNodeValue();
          String columnType = childNode.getAttributes().getNamedItem("type").getNodeValue();
          tableSchema.addColumn(new ColumnInfo(columnName, "blob".equals(columnType) ? TableSchema.BLOB_TYPE : TableSchema.DEFAULT_TYPE));
        }
      }
  
      return tableSchema;
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }
  
  private static void makeTableSchemaXml(TableSchema tableSchema, XmlWriter resultDoc) throws IOException {
    try {
      AttributesImpl attr = new AttributesImpl();
      attr.addAttribute("", "name", "", "", tableSchema.getTableName());
      resultDoc.startElement("", "table", "", attr);
  
      resultDoc.startElement("description");
      resultDoc.characters(tableSchema.getDescription());
      resultDoc.endElement("description");
      
      for (ColumnInfo column : tableSchema.getColumnInfos()) {
        attr = new AttributesImpl();
        attr.addAttribute("", "name", "", "", column.getColumnName());
        attr.addAttribute("", "type", "", "", column.getColumnType() == TableSchema.BLOB_TYPE ? "blob": "normal");
        resultDoc.startElement("", "column", "", attr);
        resultDoc.endElement("column");
      }
  
      resultDoc.endElement("table");
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }
  
  public static String getTableSchemaXml(TableSchema tableSchema) throws IOException {
    try {
      StringWriter buffer = new StringWriter();
      XmlWriter resultDoc = getXmlWriter(buffer);
      
      resultDoc.startDocument();
      resultDoc.startElement("cloudata");

      makeTableSchemaXml(tableSchema, resultDoc);
      
      resultDoc.endElement("cloudata");
      resultDoc.endDocument();
      resultDoc.flush();
      return buffer.toString();
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }
    
  public static String getRowXml(Row[] rows) throws IOException {
    try {
      StringWriter buffer = new StringWriter();
      XmlWriter resultDoc = getXmlWriter(buffer);
      
      resultDoc.startDocument();
      resultDoc.startElement("cloudata");

      for(Row row: rows) {
        makeRowXml(row, resultDoc);
      }
      
      resultDoc.endElement("cloudata");
      resultDoc.endDocument();
      resultDoc.flush();
      return buffer.toString();
    } catch (Exception e) {
      throw new IOException(e.getMessage(), e);
    }
  }
  
  public static void main(String[] args) throws Exception {
    CloudataRestService service = new CloudataRestService();

    Row row1 = new Row(new Row.Key("RK1"));
    row1.addCell("Col1", new Cell(new Cell.Key("CK_11"), "data1".getBytes()));
    row1.addCell("Col1", new Cell(new Cell.Key("CK_12"), "data2".getBytes()));
    row1.addCell("Col2", new Cell(new Cell.Key("CK_13"), "data3".getBytes()));
    
    Row row2 = new Row(new Row.Key("RK2"));
    row2.addCell("Col1", new Cell(new Cell.Key("CK_21"), "data4".getBytes()));

    service.putData("T_TEST", getRowXml(new Row[]{row1, row2}));
    
//    service.deleteTable("T_TEST");
//    service.createTable("T_TEST", getTableSchemaXml(new TableSchema("T_TEST", "test", new String[]{"Col1", "Col2"})));
  }
}