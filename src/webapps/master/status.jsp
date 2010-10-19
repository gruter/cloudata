<%@ page
  contentType="text/html; charset=UTF-8"
  import="javax.servlet.*"
  import="javax.servlet.http.*"
  import="java.io.*"
  import="java.util.*"
	import="org.cloudata.core.master.*"
%>
<%
try {
JspHelper helper = new JspHelper();
%>
<html>
<head>
<title>Cloudata</title>
<style type="text/css">
	td {font-size:9pt;}
	th {font-size:9pt;}
	body {font-size:9pt;}
	p {font-size:9pt;} 
</style>

</head>    
<body>
<%
String masterHostName = helper.getMasterHostName();
%>
<h1>CloudataMaster : <%=masterHostName%></h1>
<% helper.getClusterInfo(out); %>
<hr>
<a href="query.jsp">query</a>
<hr>
<table width="1000" border="0">
<tr>
	<td width="30%" valign="top"><% helper.getTabletServers(out); %></td>
	<td width="2%">&nbsp;</td>
	<td width="68%" valign="top"><% helper.getCommitLogServers(out); %></td>
</tr>
</table>	
<hr>
<% helper.getTables(out); %>
<hr>
<%
} catch (Exception e) {
  e.printStackTrace(new PrintWriter(out));
}
%>
Cloudata, 2010
</body>
</html>
