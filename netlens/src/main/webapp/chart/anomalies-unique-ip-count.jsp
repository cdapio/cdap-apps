<div id="uniqueIpsWithAnomaliesCount" style="width: 100%; height: 100%"></div>

<script type="text/javascript">
    $(function() {
        reloadUniqueIpsWithAnomaliesCountChart();
    });

    function reloadUniqueIpsWithAnomaliesCountChart() {
        drawUniqueIpsWithAnomaliesCountChart();
        setTimeout(function() {
            reloadUniqueIpsWithAnomaliesCountChart();
        }, 5000);
    }

    function drawUniqueIpsWithAnomaliesCountChart() {
        var startTs = Date.now() - 5000 * 120;
        var endTs = Date.now();
        $.post( "proxy/v2/apps/Netlens/procedures/AnomalyCountsProcedure/methods/uniqueIpsCount",
                        "{startTs:" + startTs + ", endTs:" + endTs + "}")
                .done(function( data ) {
                    anomalies = JSON.parse(JSON.parse(data));
                    renderUniqueIpsWithAnomaliesCountChart(anomalies);

                })
                .fail( function(xhr, textStatus, errorThrown) {
                    $('#uniqueIpsWithAnomaliesCount').html("<div class='server_error''>Failed to get data from server<div>");
                })
    }

    function renderUniqueIpsWithAnomaliesCountChart(anomalies) {
        var data = [];
        anomalies.forEach(function(anomaly){
            data.push(anomaly.value);
        });

        $('#uniqueIpsWithAnomaliesCount').highcharts({

            chart: {
                animation: false,
                type: 'column'
            },

            legend: {
                enabled: false
            },

            plotOptions: {
                series: {
                    animation: false
                }
            },

            title: {
                text: 'Unique IPs with Anomalies #'
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
                pointStart: anomalies[0].ts
            }]
        });
    };

</script>