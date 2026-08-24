import pathlib

from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent
LONG_DESCRIPTION = (HERE / "README.md").read_text(encoding="utf-8")

# Lower bounds, not pins. A hard '==' in a library forces a resolver conflict
# on anything else in the user's environment that depends on polars.
REQUIREMENTS = [
    "polars>=1.8,<2",       # diagonal_relaxed concat, collect_schema
    "requests>=2.31",
    "memoization>=0.4",
]

setup(
    name="ecdata",
    version="1.3.0",
    description=(
        "A pip installable package to distribute the "
        "Executive Communications Dataset"
    ),
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Joshua Allen",
    author_email="joshua.f.allen@gmail.com",
    license="CC-0",
    packages=find_packages(include=["ecdata", "ecdata.*"]),
    url="https://github.com/Executive-Communications-Dataset/ecdata-py",
    keywords=["Datasets", "political science", "text as data"],
    python_requires=">=3.10",
    install_requires=REQUIREMENTS,
    extras_require={"dev": ["pytest>=7", "duckdb>=0.10"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: CC0 1.0 Universal (CC0 1.0) Public Domain Dedication",
        "Intended Audience :: Science/Research",
    ],
)
