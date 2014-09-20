<div id="explanation" style="width: 100%; height: 100%"></div>

<script type="text/javascript">
    $(function() {
        reloadExplanationChart();
    });

    function reloadExplanationChart() {
        drawExplanationChart();
        setTimeout(function() {
            reloadExplanationChart();
        }, 5000);
    }

    function drawExplanationChart() {
        var key = '<%= request.getParameter("key") %>';
        if ('' == key || 'null' == key) {
            $('#explanation').html("<div style='color: #444; text-align: center; padding-top: 200px'><i>No anomaly selected</i><div>");
            return;
        }
        var fact = JSON.parse(decodeURIComponent('<%= request.getParameter("fact") %>'));
        var startTs = fact.ts - 5000 * 10;
        var endTs = fact.ts + 5000 * 10;
        $.post( "proxy/v2/apps/Netlens/procedures/CountersProcedure/methods/counts",
                        "{startTs:" + startTs + ", endTs:" + endTs + ", key:" + key + "}")
                .done(function( data ) {
                    anomalies = JSON.parse(JSON.parse(data));
                    renderExplanationChart(anomalies, fact);

                })
                .fail( function(xhr, textStatus, errorThrown) {
                    $('#explanation').html("<div class='server_error''>Failed to get data from server<div>");
                })
    }

    function renderExplanationChart(dataPoints, fact) {
        var data = [];
        dataPoints.forEach(function(dataPoint){
            data.push(dataPoint.value);
        });

        $('#explanation').highcharts({

            chart: {
                animation: false,
                type: 'line'
            },

            legend: {
                enabled: false
            },

            plotOptions: {
                series: {
                    animation: false,
                    marker: {
                        enabled: false
                    }
                }
            },

            title: {
                // hack to make nice title in highcharts
                text: 'Anomaly<br/>' +
                        '<span style="font-size: 10%; color: white">.</span><br/>' +
                        '<span style="font-size: 60%; margin-top: 4px">' + JSON.stringify(fact.dimensions) + '</span>'
            },

            xAxis: {
                type: 'datetime'
            },

            yAxis: {
                title: '',
                min: 0
            },

            tooltip: {
                headerFormat: '<b>{series.name}</b><br />',
                pointFormat: '{point.y}'
            },

            series: [{
                data: data,
                pointInterval: 5000,
                pointStart: dataPoints[0].ts
            }]
        });
    };

</script>