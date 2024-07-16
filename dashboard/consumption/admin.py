# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin

from consumption.models import Consumption, User

# Register your models here.
admin.site.register(User)
admin.site.register(Consumption)
