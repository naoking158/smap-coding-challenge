# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render

from consumption.chart.generate import (
    generate_daily_total_consumption_graph,
    generate_daily_total_consumption_graph_by_area,
)
from consumption.models import User


def summary(request):
    graph = generate_daily_total_consumption_graph()
    graph_by_area = generate_daily_total_consumption_graph_by_area()
    user_ids = list(User.objects.values_list('id', flat=True).order_by('id'))

    context = {'graph': graph, 'graph_by_area': graph_by_area, 'user_ids': user_ids}
    return render(request, 'consumption/summary.html', context)


def detail(request):
    context = {
    }
    return render(request, 'consumption/detail.html', context)
