from aiochclient import ChClient
from aiochclient.records import Record
from aiohttp import ClientSession
from typing import NoReturn, List, Optional, Any
from abc import ABC

from clickhouse_utils.query_builder import BaseSQLBuilder


class AbstractChExecutorClient(ABC):
    """
    Usage:

    click_house_client  = AbstractChExecutorClient.init_client(session, url, user, password, database)
    obj = click_house_client.get_object("table", filter_params=filter_params, fields=fields)

    objs = await click_house_client.get_list("table", filter_params=filter_params, fields=fields, pagination=pagination)

    count = await click_house_client.get_count(query)

    await click_house_client.create("table", values)

    await click_house_client.raw(query, "fetch")
    """

    sql_builder = BaseSQLBuilder

    def __init__(
        self,
        session: ClientSession,
        url: str,
        user: str,
        password: str,
        database: str,
        compress_response: bool = True,
    ) -> NoReturn:

        self.client = ChClient(
            session=session,
            url=url,
            user=user,
            password=password,
            database=database,
            compress_response=compress_response,
        )
        self.database = database

    @classmethod
    def init_client(
        cls,
        session: ClientSession,
        url: str,
        user: str,
        password: str,
        database: str,
        compress_response: bool = True,
    ):
        """
        create client for ClickHouse

        :param session: aiohttp session for connect with ClickHouse
        :param url: address to ClickHouse
        :param user: name database user
        :param password: password for user
        :param database: database name
        :param compress_response: True or False
        :return: class instance
        """
        raise NotImplementedError

    async def create(self, table: str, values: List[tuple]) -> None:
        """
        Insert data in table

        :param table: name table in database
        :param values: values which will be insert in table
        :return: None
        """
        raise NotImplementedError

    async def get_list(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        pagination: Optional[dict] = None,
        fields: Optional[List[str]] = None,
    ) -> List[Record]:
        """
        Fetch many rows from table

        :param table: name table in database
        :param filter_params: params which will be use in condition
        :param pagination: dict with values limit and offset
        :param fields: list fields which will be use in select
        :return: list records
        """
        raise NotImplementedError

    async def get_object(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        fields: Optional[List[str]] = None,
    ) -> Optional[Record]:
        """
        Fetch first row from table

        :param table: name table in database
        :param filter_params: params which will be use in condition
        :param fields: list fields which will be use in select
        :return: first row from list records
        """
        raise NotImplementedError

    async def get_count(self, query: str) -> Optional[int]:
        """
        Fetch first value of the first row from query result or None

        :param query: complete SQL query, which use how subquery
        :return: return count rows in query
        """
        raise NotImplementedError

    async def raw(self, query: str, command: str = "fetch") -> Any:
        """
        Execute complete SQL query

        :param query: complete SQL query
        :param command: one of command: "fetch", "fetchval", "execute", "fetchrow", "iterate"
        :return: depend on command
        """
        raise NotImplementedError


class ChExecutorClient(AbstractChExecutorClient):
    @classmethod
    def init_client(
        cls,
        session: ClientSession,
        url: str,
        user: str,
        password: str,
        database: str,
        compress_response: bool = True,
    ):

        return cls(session, url, user, password, database, compress_response)

    async def create(self, table: str, values: List[tuple]) -> None:

        query = self.sql_builder.insert((self.database, table), values)

        return await self.client.execute(query)

    async def get_list(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        pagination: Optional[dict] = None,
        fields: Optional[List[str]] = None,
    ) -> List[Record]:

        query = self.sql_builder.select(
            (self.database, table), filter_params, pagination, fields
        )

        return await self.client.fetch(query)

    async def get_object(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        fields: Optional[List[str]] = None,
    ) -> Optional[Record]:

        query = self.sql_builder.select(
            (self.database, table), filter_params=filter_params, fields=fields
        )

        return await self.client.fetchrow(query)

    async def get_count(self, query: str) -> Optional[int]:

        count_query = f"""SELECT count() FROM ({query}) AS c_t"""

        return await self.client.fetchval(count_query)

    async def raw(self, query: str, command: str = "fetch") -> Any:

        commands = ["fetch", "fetchval", "execute", "fetchrow", "iterate"]

        assert command in commands, "it is't accepted command"

        method = getattr(self.client, command)

        return await method(query)
