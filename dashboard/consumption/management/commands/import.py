from pathlib import Path
from typing import Any, Iterable

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils.timezone import make_aware
from pandas.api.types import is_float_dtype

from consumption.models import Consumption, User


def make_user_list_to_create_and_update(
    df: pd.DataFrame, existing_users: dict[int, User]
) -> Iterable[list[User]]:
    """User テーブルに登録するユーザーリストと、更新するユーザーリストを作成"""
    users_to_create = []
    users_to_update = []

    for _, row in df.iterrows():
        user_id = row['id']
        if user_id in existing_users:
            user = existing_users[user_id]
            user.area = row['area']
            user.tariff = row['tariff']
            users_to_update.append(user)
        else:
            users_to_create.append(User(id=row['id'], area=row['area'], tariff=row['tariff']))

    return users_to_create, users_to_update


def import_user_data(csv_file_path, batch_size=10000):
    """ユーザー情報を CSV から User テーブルへインポート"""
    df = pd.read_csv(csv_file_path)

    # 列名のチェック
    if not all(column in df.columns for column in ['id', 'area', 'tariff']):
        raise ValueError("CSV file must contain 'id', 'area', and 'tariff' columns")

    existing_users = User.objects.in_bulk(df['id'].tolist())
    users_to_create, users_to_update = make_user_list_to_create_and_update(df, existing_users)
    with transaction.atomic():
        for i in range(0, len(users_to_create) - batch_size, batch_size):
            if len(users_to_create) - i >= batch_size:
                User.objects.bulk_create(users_to_create[i : i + batch_size])
            else:
                User.objects.bulk_create(users_to_create[i:])

        for i in range(0, len(users_to_create) - batch_size, batch_size):
            if len(users_to_update) - i >= batch_size:
                User.objects.bulk_update(users_to_update[i : i + batch_size], ['area', 'tariff'])
            else:
                User.objects.bulk_update(users_to_update[i:], ['area', 'tariff'])


def load_consumption_data(consumption_dir: Path) -> pd.DataFrame:
    """消費量の情報を複数の CSV から取得して1つの pd.DataFrame に集約"""

    all_files = [consumption_dir / f for f in consumption_dir.glob('*.csv')]
    if not all_files:
        raise Exception(f'No CSV files found in {consumption_dir}')

    # 全てのCSVファイルをロード
    all_dfs = []
    for file in all_files:
        user_id = file.stem
        try:
            int(user_id)
        except ValueError:
            raise ValueError(f'Invalid user_id in filename: {file}')

        df = pd.read_csv(file)
        df['user_id'] = int(user_id)
        all_dfs.append(df)

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # 列名のチェック
    required_columns = {'user_id', 'datetime', 'consumption'}
    if not required_columns.issubset(combined_df.columns):
        raise ValueError(f'CSV file must contain columns: {required_columns}')

    # datetimeのパース
    combined_df['datetime'] = pd.to_datetime(combined_df['datetime'])
    # CSVデータにタイムゾーンの情報が含まれていない場合、UTCとして扱う
    if combined_df['datetime'].dt.tz is None:
        combined_df['datetime'] = combined_df['datetime'].apply(make_aware)

    # 重複の削除
    combined_df = combined_df.drop_duplicates(subset=['user_id', 'datetime'], keep='last')

    # consumption が float か確認
    if not is_float_dtype(combined_df['consumption']):
        try:
            combined_df['consumption'] = combined_df['consumption'].astype('float64')
        except ValueError as e:
            raise ValueError(
                f'{e}. Correct the aforementioned characters in the consumption CSV to the appropriate numerical values.'
            )
    return combined_df


def make_consumption_data_list_to_create_and_update(
    combined_df: pd.DataFrame,
    existing_consumptions: dict[Any, Consumption],
    existing_users: dict[int, User],
) -> Iterable[list[Consumption]]:
    """Consumption テーブルに登録する消費量のリストと、更新する消費量のリストを作成"""
    consumption_data_to_create = []
    consumption_data_to_update = []

    for _, row in combined_df.iterrows():
        user_id = row['user_id']
        key = (user_id, row['datetime'])
        if key in existing_consumptions:
            consumption_data = existing_consumptions[key]
            consumption_data.consumption = row['consumption']
            consumption_data_to_update.append(consumption_data)
        else:
            consumption_data_to_create.append(
                Consumption(
                    user=existing_users[user_id],
                    datetime=row['datetime'],
                    consumption=row['consumption'],
                )
            )

    return consumption_data_to_create, consumption_data_to_update


def import_all_consumption_data(consumption_dir: Path, batch_size=1000):
    """消費量の情報を複数の CSV から Consumption テーブルへインポート"""
    combined_df = load_consumption_data(consumption_dir)

    # 全ユーザIDを取得
    user_ids = combined_df['user_id'].unique().tolist()
    existing_users = User.objects.in_bulk(user_ids)

    # 登録されていないユーザIDをチェック
    for user_id in user_ids:
        if int(user_id) not in existing_users:
            raise ValueError(f'User ID {user_id} not found in database')

    # 既存の消費データを一括取得
    existing_data = Consumption.objects.filter(
        user_id__in=user_ids, datetime__in=combined_df['datetime'].tolist()
    )
    existing_consumptions = {(data.user.id, data.datetime): data for data in existing_data}

    consumption_data_to_create, consumption_data_to_update = (
        make_consumption_data_list_to_create_and_update(
            combined_df=combined_df,
            existing_consumptions=existing_consumptions,
            existing_users=existing_users,
        )
    )

    with transaction.atomic():
        for i in range(0, len(consumption_data_to_create), batch_size):
            if len(consumption_data_to_create) - i >= batch_size:
                Consumption.objects.bulk_create(consumption_data_to_create[i : i + batch_size])
            else:
                Consumption.objects.bulk_create(consumption_data_to_create[i:])

        for i in range(0, len(consumption_data_to_update), batch_size):
            if len(consumption_data_to_update) - i >= batch_size:
                Consumption.objects.bulk_update(
                    consumption_data_to_update[i : i + batch_size], ['consumption']
                )
            else:
                Consumption.objects.bulk_update(consumption_data_to_update[i:], ['consumption'])


class Command(BaseCommand):
    help = 'import data'

    def handle(self, *args, **options):
        data_dir = Path(settings.BASE_DIR).parent / 'data'
        if not data_dir.exists():
            raise FileNotFoundError(
                f'`{data_dir}` not found. Please place the directory containing the CSV files.'
            )

        import_user_data(data_dir / 'user_data.csv')

        consumption_dir = data_dir / 'consumption'
        if not consumption_dir.exists():
            raise FileNotFoundError(
                f'`{consumption_dir}` not found. Please place the directory containing the CSV files.'
            )
        import_all_consumption_data(consumption_dir)
