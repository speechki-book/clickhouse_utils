ClickHouse utils
===
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

Introduction
------------

```python
from clickhouse_utils.client import ChExecutorClient
from aiohttp import ClientSession
import datetime as dt

url = "http://clickhouse:9999"
user = "debug"
password = "debug"
database = "test"

async with ClientSession as session:
    click_house_client  = ChExecutorClient.init_client(session, url, user, password, database)

    # result query = SELECT * FROM test.table
    objs = await click_house_client.get_list("table")

    obj = await click_house_client.get_object("table")

    values = [
        (1, (dt.date(2018, 9, 7), None)),
        (2, (dt.date(2018, 9, 8), 3.14)),
    ]

    await click_house_client.create("table", values)

    query = "SELECT * FROM test.table"

    count = await click_house_client.get_count("table", query)

    raw = await click_house_client.raw(query, "fetch")

```

Installation
------------
   `$ pip install git+https://github.com/speechki-book/clickhouse_utils.git`


Dependencies
------------

1. https://github.com/speechki-book/aiochclient.git (aiochclient)
2. aiohttp
3. aiodns
4. cchardet
5. ciso8601