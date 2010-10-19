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
<title>Cloudata Tablet(<%=request.getParameter("tablet")%>)</title>
<style type="text/css">
	td {font-size:9pt;}
	th {font-size:9pt;}
	body {font-size:9pt;}
	p {font-size:9pt;} 
</style>
</head>
<body>
<%
JspHelper helper = new JspHelper();
String tabletName = request.getParameter("tablet");
String tabletServer = request.getParameter("tabletserver");

helper.getTabletDetailInfo(out, tabletName, tabletServer);
%>
<hr>
Cloudata, 2010
</body>
</html>