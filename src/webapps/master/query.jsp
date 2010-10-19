<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
	import="org.cloudata.core.master.*"
	import="org.cloudata.core.client.*"
%>
<%
	request.setCharacterEncoding("UTF-8");
	String paramTableName = request.getParameter("tableName"); 
	String[] columns = request.getParameterValues("columnName"); 
	String paramStartRowKey = request.getParameter("startRowKey");
	String paramEndRowKey = request.getParameter("endRowKey");
	String paramOperation = request.getParameter("operation");
	String paramAllVersion = request.getParameter("allVersion");
	
	String message = "";
 	JspHelper helper = new JspHelper();

 	long startTime = System.currentTimeMillis();
 	Row[] rows = null;
 	try {
 		rows = helper.runQuery(paramTableName, columns, paramStartRowKey, 
 	    paramEndRowKey, paramOperation, paramAllVersion);
 	} catch (Exception e) {
 		message = e.getMessage(); 
 	}
 	long elapsed = System.currentTimeMillis() - startTime;
 	
%>
<html>
<head>
<title>cloudata</title>
<style type="text/css">
	td {font-size:9pt;}
</style>
</head>
<body>
<a href="/status.jsp">[Goto main]</a>
<hr>
<b>Query condition</b>
<form name="form_query" action="/query.jsp" methos="POST">
<%
	helper.showQueryCondition(out, paramTableName, columns, paramStartRowKey, 
	    paramEndRowKey, paramOperation, paramAllVersion);
%>
<input type="hidden" name="tableName" value="<%=(paramTableName == null ? "" : paramTableName)%>">
</form>
<hr>
<b>Query result(<%= elapsed%> ms)</b>
<%
try {
	if(rows != null && rows.length > 0) {
	  if(columns == null || columns.length == 0) {
	    message = "No data";
	  } else {
			out.write("<table border='1' width='100%'>");	  
		  out.write("<tr>");
		  out.write("<td width=\"50\">no</td>"); 
		  out.write("<td>rowkey</td>"); 
			for(String eachColumn: columns) {
				out.write("	<td>" + eachColumn + "</td>");	  
			}
			out.write("</tr>");	  
			
			int count = 0;
			for(Row eachRow: rows) {
			  count++;
				out.write("<tr>");	
				out.write("<td valign='top'>" + count + "</td>"); 
				out.write("<td valign='top'>" + eachRow.getKey() + "</td>"); 
			  for(String eachColumn: columns) {
			    List<Cell> cells = eachRow.getCells(eachColumn);
			    if(cells == null || cells.size() == 0) {
						out.write("	<td>&nbsp;</td>");
						continue;
			    }
			    out.write("<td valign='top'>");
			    for(Cell eachCell: cells) {
			      if(eachCell == null) {
			        continue;
			      }
			      Cell.Key key = eachCell.getKey();
			      if(key == null) {
			        out.write("<b>[EMPTY]</b>"); 
			      } else {
			        out.write("<b>" + key.toString() + "</b>");
			      }
			      List<Cell.Value> values = eachCell.getValues();
			      if(values == null || values.size() == 0) {
			        out.write("<p>");
			        continue;
			      }
			      
			      for(Cell.Value eachValue: values) {
			        out.write("<br>" + eachValue.getValueAsString() + "," + eachValue.getTimestamp() + "," + (eachValue.isDeleted() ? "deleted" : ""));
			      }
			      out.write("<p>");
			    }
			    out.write("</td>");
			  }
				out.write("</tr>");	  
			}
			out.write("</table>");	  
	  }
	} else {
	  message = "No data";
	}
} catch (Exception e) {
  e.printStackTrace(new PrintWriter(out));
}
%>
<font color="red"><p><%=message%></font>
</body>
</html>