from django.urls import re_path

from . import views

urlpatterns = [
    re_path(r'^$', views.summary),
    re_path(r'^summary/', views.summary),
    re_path(r'^detail/(?P<user_id>\d+)/$', views.detail, name='detail'),
]
