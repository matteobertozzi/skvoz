#!/usr/bin/env python
#
# Copyright (c) 2012, Matteo Bertozzi
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#     notice, this list of conditions and the following disclaimer in the
#     documentation and/or other materials provided with the distribution.
#   * Neither the name of the <organization> nor the
#     names of its contributors may be used to endorse or promote products
#     derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

from json import dumps as json_dumps
from json import loads as json_loads

class Axis(object):
    SCALE_TYPE_LOGARITMIC = 0
    SCALE_TYPE_LINEAR = 1

    def __init__(self):
        self.name = None
        self.labels = None
        self.scale_type = self.SCALE_TYPE_LINEAR

    def setLabels(self, labels):
        self.labels = list(labels)

    def setScaleType(self, scale_type):
        if scale_type is None:
            scale_type = self.SCALE_TYPE_LINEAR
        self.scale_type = scale_type

class JSonChart(object):
    CHART_TYPE_COLUMN = 0
    CHART_TYPE_LINE = 1

    def __init__(self, name, gtype, legend=False):
        self.name = name
        self.gtype = gtype
        self.title = None
        self.subtitle = None
        self.legend = legend
        self.series = []
        self.xaxis = Axis()
        self.yaxis = Axis()

    def setTitle(self, title):
        self.title = title

    def setSubTitle(self, subtitle):
        self.subtitle = subtitle

    def addSerie(self, name, data):
        self.series.append((name, list(data)))

    def toJson(self):
        return {
            'name': self.name,
            'type': self.gtype,
            'title': self.title,
            'subtitle': self.subtitle,
            'series': self.series,
            'xaxis': {
                'name': self.xaxis.name,
                'labels': self.xaxis.labels,
                'scaleType': self.xaxis.scale_type
            },
            'yaxis': {
                'name': self.yaxis.name,
                'labels': self.yaxis.labels,
                'scaleType': self.yaxis.scale_type
            }
        }

    def toData(self):
        raise self.toJson()

    @classmethod
    def fromJson(cls, json_chart):
        if isinstance(json_chart, basestring):
            json_chart = json_loads(json_chart)

        chart = cls(json_chart['name'], json_chart['type'])
        chart.title = json_chart['title']
        chart.subtitle = json_chart['subtitle']
        chart.series = json_chart['series']
        chart.xaxis.name = json_chart['xaxis']['name']
        chart.xaxis.labels = json_chart['xaxis']['labels']
        chart.xaxis.scale_type = json_chart['xaxis']['scaleType']
        chart.yaxis.name = json_chart['yaxis']['name']
        chart.yaxis.labels = json_chart['yaxis']['labels']
        chart.yaxis.scale_type = json_chart['yaxis']['scaleType']
        return chart

    @classmethod
    def fromDataTable(cls, name, gtype, table, xaxis=None, yaxis=None):
        assert xaxis is not None or yaxis is not None
        series = set(table.columns)

        chart = cls(name, gtype)
        if xaxis is not None:
            series.remove(xaxis)
            chart.xaxis.setLabels(table.columnRows(xaxis))
        else:
            series.remove(yaxis)
            chart.yaxis.setLabels(table.columnRows(yaxis))

        for column in series:
            chart.addSerie(column, table.columnRows(column))

        return chart

class HighChart(JSonChart):
    NAME = "highchart"

    def toData(self):

        AXIS_MAP_TYPE = {
                Axis.SCALE_TYPE_LINEAR: 'linear',
                Axis.SCALE_TYPE_LOGARITMIC: 'logarithmic'
        }

        GTYPE_MAP_TYPE = {
            JSonChart.CHART_TYPE_COLUMN: 'column',
            JSonChart.CHART_TYPE_LINE: 'line'
        }

        def _buildAxis(axis):
            jaxis = {}
            if axis.name:
                jaxis['title'] = {'text': axis.name}
            if axis.labels:
                jaxis['categories'] = axis.labels
            if axis.scale_type is not None:
                jaxis['type'] = AXIS_MAP_TYPE.get(axis.scale_type)
            return jaxis

        chart = {'renderTo': self.name, 'type': GTYPE_MAP_TYPE[self.gtype]}
        xAxis = _buildAxis(self.xaxis)
        yAxis = _buildAxis(self.yaxis)

        jchart = {'chart': chart, 'xAxis': xAxis, 'yAxis': yAxis}
        if self.title: jchart['title'] = {'text': self.title}
        if self.subtitle: jchart['subtitle'] = {'text': self.subtitle}
        if self.series:
            jchart['series'] = [{'name': name, 'data': data}
                                for name, data in self.series]
        return json_dumps(jchart)

class GoogleChart(JSonChart):
    NAME = "google"

    def toData(self):
        GTYPE_MAP_TYPE = {
            JSonChart.CHART_TYPE_COLUMN: 'ColumnChart',
            JSonChart.CHART_TYPE_LINE: 'LineChart'
        }

        if self.xaxis.labels is not None:
            columns = self.xaxis.labels
        else:
            columns = self.yaxis.labels

        first_row = [''] + [name for name, _ in self.series]
        rows = [[None] * len(first_row) for _ in xrange(len(columns))]
        for r, col in enumerate(columns):
            rows[r][0] = col
        for c, (_, data) in enumerate(self.series):
            for r, v in enumerate(data):
                rows[r][c+1] = v
        dataTable = [first_row] + rows

        jchart = {
            'containerId': self.name,
            'chartType': GTYPE_MAP_TYPE[self.gtype],
            'dataTable': dataTable,
            'options': {
                'title': self.title,
                'vAxis': {'title': self.yaxis.name},
                'xAxis': {'title': self.xaxis.name}
            },
        }
        return json_dumps(jchart)
