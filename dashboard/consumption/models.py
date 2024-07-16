# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
from django.utils import timezone


class QuerySet(models.QuerySet):
    """
    queryset.update()で更新日時を記録するhook

    ref: https://scrapbox.io/shimizukawa/django_bulk_update_%E6%99%82%E3%81%ABupdated_at%E3%82%92%E6%9B%B4%E6%96%B0%E3%81%99%E3%82%8B
    """

    # bulk update SQLの発行元メソッド
    def update(self, **kwargs) -> int:
        if 'updated_at' not in kwargs:
            kwargs['updated_at'] = timezone.now()
        return super().update(**kwargs)


class BaseModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True, blank=True, null=True)

    # bulk_update では Model.save() が呼ばれないため、updated_at が更新されない
    # この対処として、objects を差し替える
    # ref: https://scrapbox.io/shimizukawa/django_bulk_update_%E6%99%82%E3%81%ABupdated_at%E3%82%92%E6%9B%B4%E6%96%B0%E3%81%99%E3%82%8B
    objects = models.manager.BaseManager.from_queryset(QuerySet)()

    class Meta:
        abstract = True


class User(BaseModel):
    id = models.IntegerField(primary_key=True, help_text='ユーザID')
    area = models.CharField(max_length=3, help_text='エリア')
    tariff = models.CharField(max_length=3, help_text='関税')

    def __str__(self):
        return f'User {self.id} - Area: {self.area} - Tariff: {self.tariff}'


class Consumption(models.Model):
    id = models.AutoField(primary_key=True)
    user = models.ForeignKey(
        User, on_delete=models.PROTECT, help_text='この消費データに関連するユーザ'
    )
    datetime = models.DateTimeField(help_text='消費データの日時')
    consumption = models.FloatField(help_text='30分ごとのエネルギー消費量')

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['user', 'datetime'], name='unique_user_datetime')
        ]

    def __str__(self):
        return f'User {self.user.id} - Datetime: {self.datetime} - Consumption: {self.consumption}'
