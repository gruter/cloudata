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
try {
JspHelper helper = new JspHelper();
%>
<html>
<head>
<title>cloudata</title>
<style type="text/css">
	td {font-size:9pt;}
</style>

</head>    
<body>
<h1>CloudataMaster : <%=helper.getMasterHostName()%></h1>
<% helper.getConfig(out); %>
<%
} catch (Exception e) {
  e.printStackTrace(new PrintWriter(out));
}
%>
Cloudata, 2010
</body>
</html>
