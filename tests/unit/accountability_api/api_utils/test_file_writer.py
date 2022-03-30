import tempfile

from accountability_api.api_utils import file_writer


def test_to_csv():
    f = tempfile.NamedTemporaryFile()

    rows = [
        ["col1"]  # row1
    ]

    file_writer.to_csv(rows, filename=f.name)

    with open(f.name, "r") as f:
        assert f.read() == "col1\n"


def test_to_html():
    f = tempfile.NamedTemporaryFile()

    rows = [
        ["col1"]  # row1
    ]

    file_writer.to_html(rows, filename=f.name)

    with open(f.name, "r") as f:
        assert f.read() == (
            """<!DOCTYPE html>"""
            """<html>"""
            """<body style="font-size:12px">"""
            """<table>"""
                """<style>td{padding-left: 5px;padding-right: 5px;}</style>"""
                """<tr>"""
                    """<td>"""
                        """col1"""  # note table data here
                    """</td>"""
                """</tr>"""
            """</table>"""
            """</body>"""
            """</html>"""
        )