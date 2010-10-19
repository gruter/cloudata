<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
	import="org.cloudata.core.master.*"
%>
<html>
<head>
<title>Cloudata CommiteLogServer(<%=request.getParameter("server")%>)</title>
<style type="text/css">
	td {font-size:9pt;}
</style>
</head>
<body>
<%
JspHelper helper = new JspHelper();
String server = request.getParameter("server");
helper.getCommitLogServerDetailInfos(out, server);
%>
<hr>
Cloudata, 2010
</body>
</html>