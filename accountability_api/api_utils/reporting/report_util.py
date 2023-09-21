import io
import statistics

import pandas as pd
from flask import current_app
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from pandas import Timedelta


def to_duration_isoformat(duration_seconds: float):
    td: Timedelta = pd.Timedelta(f'{int(duration_seconds)} s')
    hh = 24 * td.components.days + td.components.hours
    hhmmss_format = f"{hh:02d}:{td.components.minutes:02d}:{td.components.seconds:02d}"
    return hhmmss_format


def create_histogram(*, series: list[float], title: str, metric: str, unit: str) -> io.BytesIO:
    current_app.logger.info(f"{title=}, {len(series)=}")

    fig = Figure(layout='tight')
    ax: Axes = fig.subplots()
    ax.hist(series, bins="fd" if len(series) <= 1000 else "rice")

    # handle extreme edge case where singleton or empty series is passed
    if len(series) >= 2:
        p90 = statistics.quantiles(series, n=100, method='inclusive')[90]
        xticks = [min(*series), max(*series)] + [p90]
        ax.axvline(p90, color='k', linestyle='dashed', linewidth=1, alpha=0.5)
    elif len(series) == 1:
        xticks = series
    else:
        xticks = []

    ax.set(
        title=title,
        xlabel=f"{metric} ({unit})", xticks=xticks, xticklabels=[f"{x:.2f}" for x in xticks],
        yticks=[], yticklabels=[])

    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                 ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize('xx-small')
    histogram_img = io.BytesIO()
    current_app.logger.info(f"Saving histogram figure")
    fig.savefig(histogram_img, format="png")

    current_app.logger.info("Generated histogram")
    return histogram_img
