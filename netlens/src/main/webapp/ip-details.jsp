<!DOCTYPE html>

<!--
Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
-->

<html>
<head>
    <title>IP Details</title>
    <link rel="stylesheet" type="text/css" href="css/base.css">
    <script src="http://code.jquery.com/jquery-1.9.1.js"></script>
    <script type='text/javascript' src="http://code.highcharts.com/highcharts.js"></script>
    <script type='text/javascript' src="http://code.highcharts.com/modules/exporting.js"></script>
</head>
<body>
<div style="width: 1208px; margin-left: auto; margin-right: auto; margin-top: 20px">
    <div style="width: 1204px; height: 40px; text-align: center; font-size: 110%">
        <a href="dashboard.jsp">Dashboard</a> &nbsp;| &nbsp;
        <a href="ip-anomalies.jsp">Anomalies</a>
    </div>
    <div style="overflow:hidden">
        <div style="width: 399px; float: left; border: solid 1px #8FC7FF">
            <div style="width: 399px; height: 200px;">
                <%@include file="chart/ip-traffic.jsp"%>
            </div>
        </div>
        <div style="width: 400px; float: left; border: solid 1px #8FC7FF; border-left: 0px">
            <div style="width: 400px; height: 200px;">
                <%@include file="chart/ip-anomalies-count.jsp"%>
            </div>
        </div>

        <div style="width: 400px; float: left; border: solid 1px #8FC7FF; border-left: 0px">
            <div style="width: 400px; height: 200px; overflow: hidden">
                <%@include file="chart/ip-anomalies-list.jsp"%>
            </div>
        </div>
    </div>
    <div style="overflow:hidden">
        <div style="width: 1201px; height: 640px; border: solid 1px #8FC7FF; border-top: 0px">
            <%@include file="chart/anomaly-explained.jsp"%>
        </div>
    </div>
<div>
</body>
</html>