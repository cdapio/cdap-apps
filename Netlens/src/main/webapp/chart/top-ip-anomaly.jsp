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
        Top IPs by Anomalies #
    </div>
    <div id="topIpsWithAnomaliesList" style="width: 100%; height: 100%; overflow: scroll;"></div>
</div>

<script type="text/javascript">
    $(function() {
        reloadTopIpWithAnomaliesList();
    });

    function reloadTopIpWithAnomaliesList() {
        drawTopIpWithAnomaliesList();
        setTimeout(function() {
            reloadTopIpWithAnomaliesList();
        }, 5000);
    }

    function drawTopIpWithAnomaliesList() {
        var startTs = Date.now() - 5000 * 108;
        $.post( "proxy/v2/apps/Netlens/procedures/AnomalyCountsProcedure/methods/topN",
                        "{startTs:" + startTs + "}")
                .done(function( data ) {
                    var topN = JSON.parse(JSON.parse(data));

                    var tableHtml =
                            "<table id='topIpAnomaly_table' class='anomalies_table' align='center'>" +
                                "<tr class='anomalies_table_header'>";
                    tableHtml +=
                                "<td style='display: none'></td>" +
                                "<td class='cell'>IP</td>" +
                                "<td class='cell'>Anomalies</td>";
                    tableHtml +=
                                "</tr>";
                    if (topN.length > 0) {
                        for (i = 0; i < topN.length; i++) {
                            tableHtml += i % 2 == 0 ? "<tr>" : "<tr class='anomalies_table_even'>";
                            // Link
                            var params = $.param ({
                                fact: JSON.stringify({dimensions:{src:topN[i].value}})
                            });
                            tableHtml += "<td style='display: none'>ip-details.jsp?" + params + "</td>";
                            // IP
                            tableHtml += td(topN[i].value);
                            // Anomalies #
                            tableHtml += td(topN[i].count);

                            tableHtml += "</tr>";
                        }
                    } else {
                        tableHtml += "<tr>" + td("&nbsp;") + td("");
                        tableHtml += "</tr>";
                    }

                    tableHtml += "</table>";
                    $("#topIpsWithAnomaliesList").html(tableHtml);

                    if (topN.length > 0) {
                        $('#topIpAnomaly_table tbody').on('click', 'tr', function () {
                            var url = $('td', this).eq(0).text();
                            window.location.href=url;
                        } );
                    }

                })
                .fail( function(xhr, textStatus, errorThrown) {
                    $('#topIpsWithAnomaliesList').html("<div class='server_error''>Failed to get data from server<div>");
                })
    }

    function td(html) {
        return "<td class='cell'>" + (html == null ? "<div style='color: #888'><i>[agg]</i></div>" : html) + "</td>";
    }

</script>