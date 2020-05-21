from setuptools import setup, find_packages
import os


ROOT = os.path.dirname(__file__)


setup(
    name="clickhouse_utils",  # How you named your package folder (MyLib)
    packages=find_packages(exclude=["tests*"]),  # Chose the same as "name"
    version="0.1",  # Start with a small number and increase it with every change you make
    license="MIT",  # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    description="utils for work with clickhouse based on aiochclient",  # Give a short description about your library
    author="Pavel Maltsev",  # Type in your name
    author_email="pasha@speechki.org",  # Type in your E-Mail
    url="https://github.com/maximdanilchenko/aiochclient",  # Provide either the link to your github or to your website
    download_url="https://github.com/maximdanilchenko/aiochclient",  # I explain this later on
    keywords=[
        "ClickHouse",
        "Python",
        "AsyncIO",
        "aiochclient",
    ],  # Keywords that define your package best
    install_requires=[  # I get to this in a second
        "git+https://github.com/speechki-book/aiochclient.git#egg=aiochclient",
        "aiohttp>=3.6.2",
        "aiodns",
        "cchardet",
        "ciso8601>=2.1.1"
    ],
    classifiers=[
        "Development Status :: 4 - Beta",  # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        "Intended Audience :: Developers",  # Define that your audience are developers
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",  # Again, pick a license
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    include_package_data=True,
)
