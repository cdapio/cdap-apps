<div class="table_container">
    <div class="table_title">
        Top IPs by Traffic
    </div>
    <div id="topIpsList" style="width: 100%; height: 100%; overflow: scroll;"></div>
</div>

<script type="text/javascript">
    $(function() {
        reloadTopIpList();
    });

    function reloadTopIpList() {
        drawTopIpList();
        setTimeout(function() {
            reloadTopIpList();
        }, 5000);
    }

    function drawTopIpList() {
        var startTs = Date.now() - 5000 * 108;
        $.post( "proxy/v2/apps/Netlens/procedures/CountersProcedure/methods/topN",
                        "{startTs:" + startTs + "}")
                .done(function( data ) {
                    var topN = JSON.parse(JSON.parse(data));

                    tableHtml =
                            "<table id='topIp_table' class='anomalies_table' align='center'>" +
                                "<tr class='anomalies_table_header'>";
                    tableHtml +=
                                "<td style='display: none'></td>" +
                                "<td class='cell'>IP</td>" +
                                "<td class='cell'>Packets</td>";
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
                            // Packets #
                            tableHtml += td(topN[i].count);

                            tableHtml += "</tr>";
                        }
                    } else {
                        tableHtml += "<tr>" + td("&nbsp;") + td("");
                        tableHtml += "</tr>";
                    }

                    tableHtml += "</table>";
                    $("#topIpsList").html(tableHtml);

                    if (topN.length > 0) {
                        $('#topIp_table tbody').on('click', 'tr', function () {
                            var url = $('td', this).eq(0).text();
                            window.location.href=url;
                        } );
                    }
                })
                .fail( function(xhr, textStatus, errorThrown) {
                    $('#topIpsList').html("<div class='server_error''>Failed to get data from server<div>");
                })
    }

    function td(html) {
        return "<td class='cell'>" + (html == null ? "<div style='color: #888'><i>[agg]</i></div>" : html) + "</td>";
    }

</script>