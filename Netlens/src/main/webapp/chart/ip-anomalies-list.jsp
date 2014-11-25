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

<div class="table_container">
    <div class="table_title">
        Anomalies
    </div>
    <div id="anomaliesList" style="width: 100%; height: 100%; overflow: scroll"></div>
</div>

<script type="text/javascript">
    $(function () {
        reloadAnomaliesList();
    });

    function reloadAnomaliesList() {
        drawAnomaliesList();
        setTimeout(function () {
            reloadAnomaliesList();
        }, 5000);
    }

    function drawAnomaliesList() {
        var fact = JSON.parse(decodeURIComponent('<%= request.getParameter("fact") %>'));
        var src = fact.dimensions.src;
        var startTs = Date.now() - 5000 * 120;
        var endTs = Date.now();
        $.ajax({
            url: "proxy/v2/apps/Netlens/services/AnomaliesService/methods/timeRange/"
                    + startTs + "/" + endTs + "?groupFor=none&src=" + src,
            type: 'GET',
            contentType: "application/json",
            dataType: 'json',
            cache: false,
            success: function (data) {
                var anomalies = data;
                var tableHtml =
                        "<table id='anomalies_table' class='anomalies_table' align='center'>" +
                        "<tr class='anomalies_table_header'>";
                tableHtml +=
                        "<td class='cell'>Time</td>" +
                        "<td style='display: none'></td>" +
                        "<td class='cell'>Source IP</td>" +
                        "<td class='cell'>Destination IP</td>" +
                        "<td class='cell'>Traffic Type</td>";
                tableHtml +=
                        "</tr>";
                if (anomalies.length > 0) {
                    for (i = 0; i < anomalies.length; i++) {
                        tableHtml += i % 2 == 0 ? "<tr>" : "<tr class='anomalies_table_even'>";
                        // Link
                        var params = $.param({
                            key: anomalies[i].dataSeriesKey,
                            fact: JSON.stringify(anomalies[i].fact)
                        });
                        tableHtml += "<td style='display: none'>ip-details.jsp?" + params + "</td>";
                        // Time
                        tableHtml += td(new Date(anomalies[i].fact.ts).toLocaleTimeString());
                        // Source IP
                        tableHtml += td(anomalies[i].fact.dimensions.src);
                        // Destination IP
                        tableHtml += td(anomalies[i].fact.dimensions.dst);
                        // Traffic Type
                        tableHtml += td(anomalies[i].fact.dimensions.app);

                        tableHtml += "</tr>";
                    }
                } else {
                    tableHtml += "<tr>" + td("&nbsp;") + td("") + td("") + td("");
                    tableHtml += "</tr>";
                }

                tableHtml += "</table>";


                $("#anomaliesList").html(tableHtml);

                if (anomalies.length > 0) {
                    $('#anomalies_table tbody').on('click', 'tr', function () {
                        var url = $('td', this).eq(0).text();
                        window.location.href = url;
                    });
                }
            },
            error: function (xhr, textStatus, errorThrown) {
                $('#anomaliesList').html("<div class='server_error''>Failed to get data from server<div>");
            }
        });
    }

    function td(html) {
        return "<td class='cell'>" + (html == null ? "<div style='color: #888'><i>[agg]</i></div>" : html) + "</td>";
    }

</script>