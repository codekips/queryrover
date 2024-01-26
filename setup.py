from setuptools import find_packages, setup

VERSION = "1.0.0"
NAME = "qRover"

setup(
    name=NAME,
    packages=find_packages(include=['qRover']),
    version=VERSION,
    description='Query Rover to simplify data analysis and ingestion',
    author_email='abhayarora.works@gmail.com',
    test_suite="tests",
)