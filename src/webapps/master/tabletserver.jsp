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
<title>Cloudata TabeltServer(<%=request.getParameter("tabletserver")%>)</title>
<style type="text/css">
	td {font-size:9pt;}
	th {font-size:9pt;}
	body {font-size:9pt;}
	p {font-size:9pt;} 
</style>
<script type="text/javascript">
	function showComments() {
  	var comments = document.getElementById("comments");
  	if(comments.style.display == "none") {
    	comments.style.display = "block";
    } else {
      comments.style.display = "none"
    }
	}
</script>
</head>
<body>
<%
JspHelper helper = new JspHelper();
String tabletServer = request.getParameter("tabletserver");
helper.getTabletServerDetailInfos(out, tabletServer);
%>
<hr>
Cloudata, 2010
</body>
</html>