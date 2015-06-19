<%@ page contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" trimDirectiveWhitespaces="true"%>
<%@page import="java.util.Date"%>
<%@page import="java.text.SimpleDateFormat"%>
<%@page import="java.util.HashMap"%>
<%@page import="java.util.Map"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
    <title>Tail-Log</title>
    <style type="text/css">body { background-color: black; color: lawngreen;}</style>
    <script type="text/javascript" src="./jquery.js"></script>
</head>
<body>
<pre id="log"></pre>
</body><%
    pageContext.setAttribute("uri", new java.net.URI(request.getRequestURL().toString()));
%>
<script type="text/javascript">
    if (!window.WebSocket && window.MozWebSocket)
        window.WebSocket=window.MozWebSocket;
    if (!window.WebSocket) {
        alert("WebSocket not supported by this browser");
    }
    var conn = new WebSocket('ws://${uri.host}:${uri.port}/log');
    var log = document.getElementById("log");
    conn.onopen = function () {
        log.innerHTML = 'Socket open\n';
    };

    conn.onmessage = function (event) {
        var message = event.data;
        var total = message.length + log.innerHTML.length;
        if(total > 100000) {
            log.innerHTML = log.innerHTML.substring(10000) + message;
        } else {
            log.innerHTML += message;
        }
        var doc =  jQuery(document);
        doc.scrollTop(doc.height());
    };

    conn.onclose = function (event) {
        log.innerHTML += '\nSocket closed';
        var doc =  jQuery(document);
        doc.scrollTop(doc.height());
    };

    jQuery(window).keydown(function(event) {
        //alert(event.keyCode);
        if(event.keyCode == 83 || (event.ctrlKey && event.keyCode == 67)) {
            //S or Ctrl+C
            conn.close();
        }
    });
</script>
</html>