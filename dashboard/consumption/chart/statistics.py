import pandas as pd
from django.db import connection
from django.db.models import Sum
from django.db.models.functions import TruncDate

from consumption.models import Consumption, User


def get_daily_total_consumptions_for_all() -> pd.DataFrame:
    """全ユーザーの日ごとの消費量の合計を集計

    Returns
    -------
    pandas.DataFrame
        columns=['date', 'daily_total']
            date: 日付,
            daily_total: 全ユーザーの日ごとの消費量の合計
    """
    daily_total_consumption = (
        Consumption.objects.annotate(date=TruncDate('datetime'))
        .values('date')
        .annotate(daily_total=Sum('consumption'))
        .order_by('date')
    )
    return pd.DataFrame(daily_total_consumption)


def get_daily_percentiles_for_all() -> pd.DataFrame:
    """日ごとの消費量の 10-90%-ile と中央値を集計

    Returns
    -------
    pandas.DataFrame
        columns=['date', 'p10', 'p50', 'p90']
            date: 日付,
            p10: 全ユーザーの日ごとの消費量の 10%-ile,
            p50: 全ユーザーの日ごとの消費量の 50%-ile (median),
            p90: 全ユーザーの日ごとの消費量の 90%-ile
    """
    # モデルからテーブル名を取得
    consumption_table = Consumption._meta.db_table

    # NOTE:
    query = f"""
    WITH daily_totals AS (
        SELECT
            user_id,
            DATE_TRUNC('day', datetime) AS date,
            SUM(consumption) AS daily_total
        FROM {consumption_table}
        GROUP BY user_id, date
    )
    SELECT
        date,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY daily_total) AS p10,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_total) AS p50,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY daily_total) AS p90
    FROM daily_totals
    GROUP BY date
    ORDER BY date;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=['date', 'p10', 'p50', 'p90'])


def get_area_daily_total_consumptions() -> pd.DataFrame:
    """エリア別に日ごとの消費量の合計を集計

    Returns
    -------
    pandas.DataFrame
        columns=['area', date', 'daily_total']
            area: エリア名
            date: 日付,
            daily_total: 全ユーザーの日ごとの消費量の合計
    """
    area_daily_totals = (
        Consumption.objects.select_related('user')
        .annotate(date=TruncDate('datetime'))
        .values('user__area', 'date')
        .annotate(daily_total=Sum('consumption'))
        .order_by('user__area', 'date')
    )
    df = pd.DataFrame(area_daily_totals)
    df.rename(columns={'user__area': 'area'}, inplace=True)
    return df


def get_area_daily_percentiles() -> pd.DataFrame:
    """エリア別に日ごとの消費量の 10-90%-ile と中央値を集計

    Returns
    -------
    pandas.DataFrame
        columns=['area', 'date', 'p10', 'p50', 'p90']
            area: エリア名,
            date: 日付,
            p10: 全ユーザーの日ごとの消費量の 10%-ile,
            p50: 全ユーザーの日ごとの消費量の 50%-ile (median),
            p90: 全ユーザーの日ごとの消費量の 90%-ile
    """
    # モデルからテーブル名を取得
    consumption_table = Consumption._meta.db_table
    user_table = User._meta.db_table

    query = f"""
    WITH daily_totals AS (
        SELECT
            area,
            DATE_TRUNC('day', datetime) AS date,
            SUM(consumption) AS daily_total
        FROM {consumption_table} AS c
        INNER JOIN {user_table} AS u ON c.user_id = u.id
        GROUP BY area, user_id, date
    )
    SELECT
        area,
        date,
        PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY daily_total) AS p10,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_total) AS p50,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY daily_total) AS p90
    FROM daily_totals
    GROUP BY area, date
    ORDER BY area, date;
    """

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()

    return pd.DataFrame(rows, columns=['area', 'date', 'p10', 'p50', 'p90'])


def get_user_daily_total_consumptions(user_id: int) -> pd.DataFrame:
    """特定ユーザーの日ごとの消費量の合計を集計

    Params
    ------
    user_id : int
        対象ユーザのID

    Returns
    -------
    pandas.DataFrame
        columns=['date', 'daily_total']
            date: 日付,
            daily_total: ユーザーの日ごとの消費量の合計
    """
    user_daily_totals = (
        Consumption.objects.filter(user_id=user_id)
        .annotate(date=TruncDate('datetime'))
        .values('date')
        .annotate(daily_total=Sum('consumption'))
        .order_by('date')
    )
    return pd.DataFrame(user_daily_totals)


def get_user_area_daily_consumption_median(user_id: int) -> pd.DataFrame:
    """特定ユーザが属するエリアの日ごとの消費量の中央値を集計

    Params
    ------
    user_id : int
        対象ユーザのID

    Returns
    -------
    pandas.DataFrame
        columns=['date', 'p10', 'p50', 'p90']
            date: 日付,
            p50: 全ユーザーの日ごとの消費量の 50%-ile (median)
    """
    # モデルからテーブル名を取得
    consumption_table = Consumption._meta.db_table
    user_table = User._meta.db_table

    query = f"""
    WITH daily_totals AS (
        SELECT
            DATE_TRUNC('day', datetime) AS date,
            SUM(consumption) AS daily_total
        FROM {consumption_table} AS c
        INNER JOIN {user_table} AS u ON c.user_id = u.id
        WHERE u.area = (SELECT area FROM {user_table} WHERE id = %s)
        GROUP BY user_id, date
    )
    SELECT
        date,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY daily_total) AS p50
    FROM daily_totals
    GROUP BY date
    ORDER BY date;
    """

    with connection.cursor() as cursor:
        cursor.execute(query, [user_id])
        area_rows = cursor.fetchall()

    return pd.DataFrame(area_rows, columns=['date', 'p50'])
