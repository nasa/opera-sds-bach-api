"""Check the 90th-percentile reference shown in generated report histograms."""

import base64
from copy import deepcopy
from io import BytesIO

import numpy as np
import pytest
from flask import Flask
from matplotlib.figure import Figure
from PIL import Image

from accountability_api.api_utils.reporting import report_util
from accountability_api.api_utils.reporting.production_time_report import (
    ProductionTimeReport,
)


@pytest.fixture
def plot_context(mocker):
    app = Flask(__name__)
    figure = Figure(layout="tight")
    # Use real axes and rendering, retaining the figure for data assertions.
    mocker.patch.object(report_util, "Figure", return_value=figure)
    with app.app_context():
        yield figure


def assert_png(buffer):
    buffer.seek(0)
    with Image.open(buffer) as image:
        assert image.format == "PNG"
        assert image.width > 0 and image.height > 0
        image.load()


@pytest.mark.parametrize(
    "values",
    [
        list(range(101)),
        [0.0, 100.0],
        [10.0, 80.0, 20.0, 40.0],
        [0.0] * 9 + [100.0],
        [-10.0, -5.0, -1.0, -0.5],
        [0.125, 0.25, 0.5, 0.875],
        list(range(1001)),
    ],
)
def test_histogram_marks_the_ninetieth_percentile(plot_context, values):
    original = values.copy()
    expected = np.percentile(values, 90)
    buffer = report_util.create_histogram(
        series=values, title="Latency distribution", metric="Latency", unit="hours"
    )
    axis = plot_context.axes[0]
    assert len(axis.lines) == 1
    np.testing.assert_allclose(axis.lines[0].get_xdata(), [expected, expected])
    np.testing.assert_allclose(axis.get_xticks(), [min(values), max(values), expected])
    assert axis.get_xticklabels()[-1].get_text() == f"{expected:.2f}"
    assert axis.get_title() == "Latency distribution"
    assert axis.get_xlabel() == "Latency (hours)"
    assert values == original
    assert_png(buffer)


@pytest.mark.parametrize("values", [[], [4.0], [4.0, 4.0, 4.0]])
def test_empty_singleton_and_constant_series_still_render(plot_context, values):
    buffer = report_util.create_histogram(
        series=values, title="Small sample", metric="Latency", unit="s"
    )
    axis = plot_context.axes[0]
    assert len(axis.lines) == (1 if len(values) >= 2 else 0)
    if values:
        np.testing.assert_allclose(
            axis.get_xticks(), [4.0] * (3 if len(values) >= 2 else 1)
        )
    else:
        assert len(axis.get_xticks()) == 0
    assert_png(buffer)


def test_production_summary_embeds_the_correct_histogram(plot_context):
    products = [
        {
            "daac_CNM_S_timestamp": f"2026-01-01T{hour:02d}:00:00Z",
            "metadata": {
                "ProductReceivedTime": "2026-01-01T00:00:00Z",
                "FileName": f"sample-{hour}",
                "ProductType": "example",
            },
        }
        for hour in (1, 2, 3, 4)
    ]
    original = deepcopy(products)
    frame = ProductionTimeReport.to_report_df(
        product_docs=products,
        report_type="summary",
        report_options={"generate_histograms": True},
    )
    assert len(frame) == 1
    assert frame.iloc[0]["production_time_count"] == 4
    assert frame.iloc[0]["production_time_mean"] == "02:30:00"
    np.testing.assert_allclose(plot_context.axes[0].lines[0].get_xdata(), [3.7, 3.7])

    assert_png(BytesIO(base64.b64decode(frame.iloc[0]["histogram"])))
    assert products == original
