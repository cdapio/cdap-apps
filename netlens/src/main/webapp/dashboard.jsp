<!DOCTYPE html>
<html>
<head>
    <title>Dashboard</title>
    <link rel="stylesheet" type="text/css" href="css/base.css">
    <script src="http://code.jquery.com/jquery-1.9.1.js"></script>
    <script type='text/javascript' src="http://code.highcharts.com/highcharts.js"></script>
    <script type='text/javascript' src="http://code.highcharts.com/modules/exporting.js"></script>
</head>
<body>
<div style="width: 1208px; margin-left: auto; margin-right: auto; margin-top: 20px">
    <div style="width: 1204px; height: 40px; text-align: center; font-size: 110%">
        <b>Dashboard</b> &nbsp;| &nbsp;
        <a href="ip-anomalies.jsp">Anomalies</a>
    </div>
    <div style="overflow:hidden;">
        <div style="width: 600px; float: left; border: solid 1px #8FC7FF">
            <div style="width: 600px; height: 200px;">
                <%@include file="chart/traffic.jsp"%>
            </div>
        </div>
        <div style="width: 600px; border: solid 1px #8FC7FF; border-left: 0px; float: left">
            <div style="width: 600px; height: 200px;">
                <%@include file="chart/anomalies-count.jsp"%>
            </div>
        </div>
    </div>
    <div style="overflow:hidden">
        <div style="width: 199px; border: solid 1px #8FC7FF; border-top: 0px; float: left">
            <div style="width: 199px; height: 640px; overflow: hidden">
                <%@include file="chart/top-ip-traffic.jsp"%>
            </div>
        </div>
        <div style="width: 199px; border: solid 1px #8FC7FF; border-top: 0px; border-left: 0px; float: left">
            <div style="width: 199px; height: 640px; overflow: hidden">
                <%@include file="chart/top-ip-anomaly.jsp"%>
            </div>
        </div>
        <div style="width: 801px; border: solid 1px #8FC7FF; border-top: 0px; border-left: 0px; float: left">
            <div style="width: 801px; height: 640px; overflow: hidden">
                <%
                    request.setAttribute("shorten", "true");
                %>
                <%@include file="chart/anomalies-list.jsp"%>
            </div>
        </div>
    </div>
</div>
</body>
</html>