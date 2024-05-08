from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from setuptools import setup, find_packages

setup(
    name="accountability_api",
    version="2.0.0",
    long_description="Data Accountability REST API using ElasticSearch backend",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # periodically update the minimum versions of these with what's in the deployment environment
        "Flask>=2.2.5",
        "Flask-Compress>=1.15",
        "Flask-Cors>=4.0.1",
        "flask-restx>=1.3.0",

        "more-itertools>=10.2.0",
        "elasticsearch>=7.13.4,<8.0.0",
        "pandas>=2.1.0,== 2.*",
        "matplotlib>=3.7.2",
    ],
    extras_require={
        'test': [
            "pytest>=7.4.2",
            "pytest-mock",
            "coverage",
            "pytest-cov",

            "prov-es@https://github.com/hysds/prov_es/archive/refs/tags/v0.2.2.tar.gz",
            "osaka@https://github.com/hysds/osaka/archive/refs/tags/v1.1.0.tar.gz",
            "hysds-commons@https://github.com/hysds/hysds_commons/archive/refs/tags/v1.0.9.tar.gz",
            "hysds@https://github.com/hysds/hysds/archive/refs/tags/v1.1.5.tar.gz"
        ]
    }
)
