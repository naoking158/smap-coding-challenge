# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import numpy as np
import pandas as pd
from django.test import TestCase
from django.utils import timezone

from consumption.chart.statistics import (
    get_area_daily_percentiles,
    get_area_daily_total_consumptions,
    get_daily_percentiles_for_all,
    get_daily_total_consumptions_for_all,
    get_user_area_daily_consumption_median,
    get_user_daily_total_consumptions,
)
from consumption.models import Consumption, User


class StatisticsTests(TestCase):
    def setUp(self):
        # ユーザーを作成
        self.users = [
            User.objects.create(id=1, area='a1', tariff='t1'),
            User.objects.create(id=2, area='a2', tariff='t3'),
            User.objects.create(id=3, area='a1', tariff='t1'),
            User.objects.create(id=4, area='a2', tariff='t3'),
        ]

        # 基準時刻を作成
        std_time = timezone.now().replace(hour=0, minute=0, second=0, microsecond=0)

        # 消費データの値をまとめる配列
        self.consumption_values = []

        # 3日分のデータを30分おきに作成
        for day in range(3):
            for half_hour in range(48):
                datetime = (
                    std_time
                    - timezone.timedelta(days=day)
                    + timezone.timedelta(minutes=30 * half_hour)
                )
                for i, user in enumerate(self.users):
                    self.consumption_values.append((user, datetime, 10.0 * (i + 1) + half_hour))

        # 消費データを作成
        for user, datetime, consumption in self.consumption_values:
            Consumption.objects.create(user=user, datetime=datetime, consumption=consumption)

    def test_get_daily_total_consumptions_for_all(self):
        df = get_daily_total_consumptions_for_all()

        # 消費データを日付ごとに集計
        consumption_by_date = {}
        for _, datetime, consumption in self.consumption_values:
            date = datetime.astimezone(timezone.get_default_timezone()).date()
            if date not in consumption_by_date:
                consumption_by_date[date] = 0
            consumption_by_date[date] += consumption

        # 期待されるデータフレームを作成
        expected_data = [
            {'date': date, 'daily_total': total}
            for date, total in sorted(consumption_by_date.items())
        ]
        expected_df = pd.DataFrame(expected_data)

        # 期待されるデータフレームと関数の結果を比較
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_daily_percentiles_for_all(self):
        df = get_daily_percentiles_for_all()

        # 日付ごとの消費データをユーザーごとに集計
        consumption_by_date_user = {}
        for user, datetime, consumption in self.consumption_values:
            date = datetime.astimezone(timezone.get_default_timezone()).date()
            if date not in consumption_by_date_user:
                consumption_by_date_user[date] = {}
            if user.id not in consumption_by_date_user[date]:
                consumption_by_date_user[date][user.id] = 0
            consumption_by_date_user[date][user.id] += consumption

        # 期待されるデータフレームを作成
        expected_data = []
        for date, user_consumptions in consumption_by_date_user.items():
            daily_totals = list(user_consumptions.values())
            p10 = np.percentile(daily_totals, 10)
            p50 = np.percentile(daily_totals, 50)
            p90 = np.percentile(daily_totals, 90)
            expected_data.append({'date': date, 'p10': p10, 'p50': p50, 'p90': p90})

        expected_df = pd.DataFrame(expected_data).sort_values('date').reset_index(drop=True)
        expected_df['date'] = pd.to_datetime(expected_df['date']).dt.tz_localize(
            timezone.get_default_timezone()
        )

        # 期待されるデータフレームと関数の結果を比較
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_area_daily_total_consumptions(self):
        df = get_area_daily_total_consumptions()

        # エリアごと、日付ごとの消費データを集計
        consumption_by_area_date = {}
        for user, datetime, consumption in self.consumption_values:
            date = datetime.astimezone(timezone.get_default_timezone()).date()
            area = user.area
            if area not in consumption_by_area_date:
                consumption_by_area_date[area] = {}
            if date not in consumption_by_area_date[area]:
                consumption_by_area_date[area][date] = 0
            consumption_by_area_date[area][date] += consumption

        # 期待されるデータフレームを作成
        expected_data = []
        for area, date_consumptions in consumption_by_area_date.items():
            for date, total in date_consumptions.items():
                expected_data.append({'area': area, 'date': date, 'daily_total': total})

        expected_df = (
            pd.DataFrame(expected_data).sort_values(['area', 'date']).reset_index(drop=True)
        )

        # 期待されるデータフレームと関数の結果を比較
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_area_daily_percentiles(self):
        df = get_area_daily_percentiles()

        # エリアごと、日付ごとの消費データをユーザーごとに集計
        consumption_by_area_date_user = {}
        for user, datetime, consumption in self.consumption_values:
            date = datetime.astimezone(timezone.get_default_timezone()).date()
            area = user.area
            if area not in consumption_by_area_date_user:
                consumption_by_area_date_user[area] = {}
            if date not in consumption_by_area_date_user[area]:
                consumption_by_area_date_user[area][date] = {}
            if user.id not in consumption_by_area_date_user[area][date]:
                consumption_by_area_date_user[area][date][user.id] = 0
            consumption_by_area_date_user[area][date][user.id] += consumption

        # 期待されるデータフレームを作成
        expected_data = []
        for area, date_consumptions in consumption_by_area_date_user.items():
            for date, user_consumptions in date_consumptions.items():
                daily_totals = list(user_consumptions.values())
                p10 = np.percentile(daily_totals, 10)
                p50 = np.percentile(daily_totals, 50)
                p90 = np.percentile(daily_totals, 90)
                expected_data.append(
                    {'area': area, 'date': date, 'p10': p10, 'p50': p50, 'p90': p90}
                )

        expected_df = (
            pd.DataFrame(expected_data).sort_values(['area', 'date']).reset_index(drop=True)
        )
        expected_df['date'] = pd.to_datetime(expected_df['date']).dt.tz_localize(
            timezone.get_default_timezone()
        )

        # 期待されるデータフレームと関数の結果を比較
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_user_daily_total_consumptions(self):
        user_id = self.users[0].id
        df = get_user_daily_total_consumptions(user_id)

        # 特定ユーザーの日付ごとの消費データを集計
        consumption_by_date = {}
        for user, datetime, consumption in self.consumption_values:
            if user.id == user_id:
                date = datetime.astimezone(timezone.get_default_timezone()).date()
                if date not in consumption_by_date:
                    consumption_by_date[date] = 0
                consumption_by_date[date] += consumption

        # 期待されるデータフレームを作成
        expected_data = [
            {'date': date, 'daily_total': total}
            for date, total in sorted(consumption_by_date.items())
        ]
        expected_df = pd.DataFrame(expected_data)

        # 期待されるデータフレームと関数の結果を比較
        pd.testing.assert_frame_equal(df, expected_df)

    def test_get_user_area_daily_consumption_median(self):
        user_id = self.users[0].id
        df = get_user_area_daily_consumption_median(user_id)

        # 特定ユーザーが属するエリアの日付ごとの消費データをユーザーごとに集計
        user_area = self.users[0].area
        consumption_by_date_user = {}
        for user, datetime, consumption in self.consumption_values:
            if user.area == user_area:
                date = datetime.astimezone(timezone.get_default_timezone()).date()
                if date not in consumption_by_date_user:
                    consumption_by_date_user[date] = {}
                if user.id not in consumption_by_date_user[date]:
                    consumption_by_date_user[date][user.id] = 0
                consumption_by_date_user[date][user.id] += consumption

        # 期待されるデータフレームを作成
        expected_data = []
        for date, user_consumptions in consumption_by_date_user.items():
            daily_totals = list(user_consumptions.values())
            p50 = np.percentile(daily_totals, 50)
            expected_data.append({'date': date, 'p50': p50})

        expected_df = pd.DataFrame(expected_data).sort_values('date').reset_index(drop=True)
        expected_df['date'] = pd.to_datetime(expected_df['date']).dt.tz_localize(
            timezone.get_default_timezone()
        )

        # 期待されるデータフレームと関数の結果を比較
        pd.testing.assert_frame_equal(df, expected_df)
