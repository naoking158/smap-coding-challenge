from pathlib import Path
from typing import Iterable

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction

from consumption.models import User


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



class Command(BaseCommand):
    help = 'import data'

    def handle(self, *args, **options):
        data_dir = Path(settings.BASE_DIR).parent / 'data'
        if not data_dir.exists():
            raise FileNotFoundError(
                f'`{data_dir}` not found. Please place the directory containing the CSV files.'
            )

        import_user_data(data_dir / 'user_data.csv')

