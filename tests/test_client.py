from unittest.mock import patch
import pytest

from aiohttp import ClientSession
from clickhouse_utils.client import ChExecutorClient, AbstractChExecutorClient
from aiochclient.client import ChClient


@pytest.mark.asyncio
async def test_init_client():
    url = "http://clickhouse:8192"
    user = "debug"
    password = "debug"
    database = "test"
    compress_response = True

    check_settings = {
        "user": user,
        "password": password,
        "database": database,
        "enable_http_compression": 1
    }

    async with ClientSession() as session:
        client = ChExecutorClient.init_client(session, url, user, password, database, compress_response)

        assert isinstance(client, AbstractChExecutorClient), "client not AbstractChExecutorClient child"

        assert client.database == database, "database not eq"

        assert isinstance(client.client, ChClient), "client for execute query must be ChClient"

        assert url == client.client.url, "url in client not eq check url"

        for key, value in check_settings.items():
            assert client.client.params.get(key) == value, f"check value for {key} not eq params in client"


