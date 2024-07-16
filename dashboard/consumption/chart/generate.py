import base64
import io

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from consumption.chart.statistics import (
    get_area_daily_percentiles,
    get_area_daily_total_consumptions,
    get_daily_percentiles_for_all,
    get_daily_total_consumptions_for_all,
    get_user_area_daily_consumption_median,
    get_user_daily_total_consumptions,
)


def plot_total_consumption(df: pd.DataFrame, percentiles: pd.DataFrame) -> Figure:
    fig, ax1 = plt.subplots(figsize=(10, 5))

    ax1.plot(df['date'], df['daily_total'], label='Total Consumption', color='blue')
    ax1.set_title('Daily Consumption with 10-90 Percentile and Median')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Total Consumption', color='blue')
    ax1.tick_params(axis='y', labelcolor='blue')
    ax1.grid(True)

    ax2 = ax1.twinx()
    if not percentiles.empty:
        ax2.fill_between(
            percentiles['date'],
            percentiles['p10'],
            percentiles['p90'],
            color='green',
            alpha=0.1,
            label='10-90 Percentile',
        )
        ax2.plot(
            percentiles['date'], percentiles['p50'], linestyle='--', label='Median', color='green'
        )
    ax2.set_ylabel('Percentiles and Median', color='green')
    ax2.tick_params(axis='y', labelcolor='green')

    # 凡例の統合
    lines, labels = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines + lines2, labels + labels2, loc='upper left', bbox_to_anchor=(0.1, 0.9))

    return fig


def plot_area_consumption(area_totals: pd.DataFrame, area_percentiles: pd.DataFrame) -> Figure:
    fig, ax = plt.subplots(figsize=(10, 5))

    colors = ['red', 'cyan', 'green', 'blue']
    color_index = 0
    ax2 = ax.twinx()
    for area in area_totals['area'].unique():
        area_data_totals = area_totals[area_totals['area'] == area]
        area_data_percentiles = area_percentiles[area_percentiles['area'] == area]

        ax.plot(
            area_data_totals['date'],
            area_data_totals['daily_total'],
            label=f'{area} Total Consumption',
            color=colors[color_index],
        )
        ax2.fill_between(
            area_data_percentiles['date'],
            area_data_percentiles['p10'],
            area_data_percentiles['p90'],
            alpha=0.1,
            label=f'{area} 10-90 Percentile',
            color=colors[color_index],
        )
        ax2.plot(
            area_data_percentiles['date'],
            area_data_percentiles['p50'],
            linestyle='--',
            label=f'{area} Median',
            color=colors[color_index],
        )

        color_index = (color_index + 1) % len(colors)

    ax.set_title('Daily Consumption with 10-90 Percentile and Median by Area')
    ax.set_xlabel('Date')
    ax.set_ylabel('Total Consumption')
    ax.grid(True)

    ax2.set_ylabel('Percentiles and Median')
    lines, labels = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines + lines2, labels + labels2, loc='upper left', bbox_to_anchor=(0.1, 0.9))

    return fig


def plot_user_and_area_consumption(
    user_df: pd.DataFrame, area_df: pd.DataFrame, user_id: int
) -> Figure:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(
        user_df['date'], user_df['daily_total'], label=f'User {user_id} Consumption', color='blue'
    )
    ax.plot(
        area_df['date'],
        area_df['p50'],
        label='Area Median Consumption',
        color='red',
        linestyle='--',
    )
    ax.set_xlabel('Date')
    ax.set_ylabel('Total Consumption')
    ax.grid(True)
    ax.legend(loc='upper left')

    return fig


def generate_daily_total_consumption_graph() -> str:
    """日ごとの消費量の総量と、中央値と 10-90%-ile をプロットしたグラフを生成"""
    df = get_daily_total_consumptions_for_all()
    percentiles = get_daily_percentiles_for_all()

    with io.BytesIO() as buffer:
        fig = plot_total_consumption(df, percentiles)
        fig.savefig(buffer, format='png')
        buffer.seek(0)
        image_png = buffer.getvalue()

    graph = base64.b64encode(image_png).decode('utf-8')
    return graph


def generate_daily_total_consumption_graph_by_area() -> str:
    """エリア別に、日ごとの消費量の総量と、中央値と 10-90%-ile をプロットしたグラフを生成"""
    df = get_area_daily_total_consumptions()
    percentiles = get_area_daily_percentiles()

    with io.BytesIO() as buffer:
        fig = plot_area_consumption(df, percentiles)
        fig.savefig(buffer, format='png')
        buffer.seek(0)
        image_png = buffer.getvalue()

    graph = base64.b64encode(image_png).decode('utf-8')
    return graph


def generate_user_consumption_graph(user_id: int) -> str:
    """ユーザーごとの日ごとの消費量の総量と、エリアの中央値をプロットしたグラフを生成"""
    user_df = get_user_daily_total_consumptions(user_id)
    area_df = get_user_area_daily_consumption_median(user_id)

    with io.BytesIO() as buffer:
        fig = plot_user_and_area_consumption(user_df, area_df, user_id)
        fig.savefig(buffer, format='png')
        buffer.seek(0)
        image_png1 = buffer.getvalue()

    graph = base64.b64encode(image_png1).decode('utf-8')
    return graph
