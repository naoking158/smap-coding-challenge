import base64
import io

import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.figure import Figure

from consumption.chart.statistics import (
    get_daily_percentiles_for_all,
    get_daily_total_consumptions_for_all,
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
