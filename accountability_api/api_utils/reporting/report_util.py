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
    current_app.logger.info(f"{len(series)=}")

    fig = Figure(layout='tight')
    ax: Axes = fig.subplots()
    ax.hist(series, bins='auto')

    p90 = statistics.quantiles(series, n=100, method='inclusive')[90]
    xticks = [p90]

    # extreme edge case where singleton series is passed
    if len(series) >= 2:
        xticks = xticks + [min(*series), max(*series)]

    ax.set(
        title=title,
        xlabel=f"{metric} ({unit})", xticks=xticks, xticklabels=[f"{x:.2f}" for x in xticks],
        yticks=[], yticklabels=[])

    ax.axvline(p90, color='k', linestyle='dashed', linewidth=1, alpha=0.5)
    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label] +
                 ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize('xx-small')
    histogram_img = io.BytesIO()
    fig.savefig(histogram_img, format="png")

    current_app.logger.info("Generated histogram")
    return histogram_img
