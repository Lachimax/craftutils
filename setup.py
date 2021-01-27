import setuptools
from shutil import copy
from os import getcwd

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="craftutils",
    version="0.9.0",
    author="Lachlan Marnoch",
    short_description=long_description,
    long_description=long_description,
    url="https://github.com/Lachimax/craftutils",
    packages=setuptools.find_packages(),
    python_requires='>=3.6'
)
