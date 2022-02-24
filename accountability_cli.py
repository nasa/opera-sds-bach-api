import click
import requests
import time
import json

from datetime import datetime, timedelta
from lxml import etree

from hysds.celery import app


GRQ_URL = ":".join(app.conf["GRQ_ES_URL"].split(":")[0:-1])
default_start = datetime.utcnow().isoformat()
default_end = (datetime.utcnow() + timedelta(days=365 * 4)).isoformat()


def get_json_metadata(response_text):
    json_data = json.loads(response_text)

    return json_data["HEADER"]


def get_xml_metadata(response_text):
    root = etree.fromstring(response_text)
    header = root.find("HEADER")

    metadata = {}
    for item in header.iterchildren():
        metadata[item.tag] = item.text

    return metadata


def write_oar_report(metadata, response_text, format_type):
    file_name = "oar_{}_{}_{}.{}".format(
        metadata["CONTENT_TYPE"],
        metadata["START_DATETIME"],
        metadata["END_DATETIME"],
        format_type,
    )

    with open(file_name, "w") as f:
        f.write(response_text)

    return file_name


@click.command()
@click.option("--format_type", default="xml", help="format to return the result")
@click.option("--start", default=default_start, help="UTC start datetime in iso format")
@click.option("--end", default=default_end, help="UTC end datetime in iso format")
@click.argument("report_name")
def get_report(format_type, start, end, report_name):
    if "Z" not in start:
        start = "{}Z".format(start)

    if "Z" not in end:
        end = "{}Z".format(end)

    reports_url = (
        GRQ_URL
        + ":8875/1.0/reports/{}?startDateTime={}&endDateTime={}&mime={}".format(
            report_name, start, end, format_type
        )
    )
    click.echo("Fetching report from Bach-API")
    # wait for the response
    response = requests.get(reports_url)
    # retrieved response
    waited = response.elapsed
    time.sleep(0.5)
    while waited < response.elapsed:
        click.echo(".", nl=False)
        waited == response.elapsed
        time.sleep(0.5)
    click.echo(".", nl=True)
    metadata = {}

    if format_type == "xml":
        metadata = get_xml_metadata(response.text)
    elif format_type == "json":
        metadata = get_json_metadata(response.text)
    else:
        click.echo("Not Implemented")
        raise NotImplementedError("%s data format not implemented" % format_type)

    if response.status_code == "501":
        raise NotImplementedError("%s report not implemented" % report_name)
    if report_name == "ObservationAccountabilityReport":
        filename = write_oar_report(metadata, response.text, format_type)
        click.echo("wrote out %s" % filename)
    else:
        raise NotImplementedError("%s report not implemented" % report_name)


if __name__ == "__main__":
    get_report()
