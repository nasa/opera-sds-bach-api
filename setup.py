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
        "elasticsearch>=7.13.4",
        "pandas>=2.1.0,== 2.*",
        "matplotlib>=3.7.2",

        "aws-requests-auth==0.4.3"
    ],
    extras_require={
        'test': [
            "pytest>=7.4.2",
            "pytest-mock",
            "coverage",
            "pytest-cov",

            "prov-es@https://github.com/hysds/prov_es/archive/refs/tags/v0.3.0.tar.gz",
            "osaka@https://github.com/hysds/osaka/archive/refs/tags/v1.3.0.tar.gz",
            "hysds-commons@https://github.com/hysds/hysds_commons/archive/refs/tags/v1.2.0.tar.gz",
            "hysds@https://github.com/hysds/hysds/archive/refs/tags/v1.4.0.tar.gz"
        ]
    }
)
