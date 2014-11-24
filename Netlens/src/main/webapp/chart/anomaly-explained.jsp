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
    <div id="explanationTitle" class="table_title">
        No anomaly selected
    </div>
    <div id="explanation" style="width: 100%; height: 80%; margin-top: 20px;">&nbsp;</div>
</div>

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
            $('#explanationTitle').html("<i>No anomaly selected</i>");
            return;
        }
        var fact = JSON.parse(decodeURIComponent('<%= request.getParameter("fact") %>'));
        var title= '<div>Anomaly<div/>' +
                   '<div style="font-size: 80%; margin-top: 12px">' + JSON.stringify(fact.dimensions) + '</div>';
        $('#explanationTitle').html(title);

        var startTs = fact.ts - 5000 * 10;
        var endTs = fact.ts + 5000 * 10;
        var url = "proxy/v2/apps/Netlens/services/CountersService/methods/counts/"
                + startTs + "/" + endTs + "?key=" + key;
        $.post(url)
                .done(function( data ) {
                    renderExplanationChart(data);
                })
                .fail( function(xhr, textStatus, errorThrown) {
                    $('#explanation').html("<div class='server_error''>Failed to get data from server<div>");
                })
    }

    function renderExplanationChart(dataPoints) {
        var data = [];
        dataPoints.forEach(function(point){
            data.push([point.ts, point.value]);
        });

        var plot = $.plot("#explanation", [data], {
            series: { shadowSize: 0 },
            yaxis: { min: 0 },
            xaxis: { mode: 'time'},
            grid: {borderWidth: 0}
        });

        plot.draw();
    }

</script>