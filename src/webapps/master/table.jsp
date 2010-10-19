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
<title>cloudata</title>
<style type="text/css">
	td {font-size:9pt;}
</style>
</head> 
<body>
<%
try {
	JspHelper helper = new JspHelper();
	String table = request.getParameter("table");
	String pageStr = request.getParameter("page");
	int currentPage = 1;
	if(pageStr != null && pageStr.length() > 0) {
	  currentPage = Integer.parseInt(pageStr);
	}
	helper.getTableInfo(out, table, currentPage);
} catch (Exception e) {  
  e.printStackTrace(new PrintWriter(out));
}
%>
<hr>
Cloudata, 2010
</body>
</html>