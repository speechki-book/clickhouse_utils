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

    async def create(self, table: str, values: List[tuple], **kwargs) -> None:
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
        ordering: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Record]:
        """
        Fetch many rows from table

        :param table: name table in database
        :param filter_params: params which will be use in condition
        :param pagination: dict with values limit and offset
        :param fields: list fields which will be use in select
        :param ordering: ORDER BY fields
        :return: list records
        """
        raise NotImplementedError

    async def get_object(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        fields: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[Record]:
        """
        Fetch first row from table

        :param table: name table in database
        :param filter_params: params which will be use in condition
        :param fields: list fields which will be use in select
        :return: first row from list records
        """
        raise NotImplementedError

    async def get_count(
        self,
        query: Optional[str] = None,
        table: Optional[str] = None,
        filter_params: Optional[dict] = None,
        fields: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[int]:
        """
        Fetch first value of the first row from query result or None

        Don't use query if contain table. Use case - pagination

        :param query: complete SQL query, which use how subquery
        :param table: table name in database
        :param filter_params: params which will be use in condition
        :param fields: list fields which will be use in select
        :return: return count rows in query
        """
        raise NotImplementedError

    async def raw(self, query: str, command: str = "fetch", **kwargs) -> Any:
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

    async def create(
        self, table: str, values: List[tuple], fields: List[str] = None, **kwargs
    ) -> None:

        query = self.sql_builder.insert((self.database, table), values, fields)

        return await self.client.execute(query)

    async def get_list(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        pagination: Optional[dict] = None,
        fields: Optional[List[str]] = None,
        ordering: Optional[List[str]] = None,
        **kwargs,
    ) -> List[Record]:

        query = self.sql_builder.select(
            (self.database, table), filter_params, pagination, fields, ordering
        )

        return await self.client.fetch(query)

    async def get_object(
        self,
        table: str,
        filter_params: Optional[dict] = None,
        fields: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[Record]:

        query = self.sql_builder.select(
            (self.database, table), filter_params=filter_params, fields=fields
        )

        return await self.client.fetchrow(query)

    async def get_count(
        self,
        query: Optional[str] = None,
        table: Optional[str] = None,
        filter_params: Optional[dict] = None,
        fields: Optional[List[str]] = None,
        **kwargs,
    ) -> Optional[int]:

        assert (query is not None) or (table is not None), "must be use query or table"

        if table:
            query = self.sql_builder.select(
                (self.database, table), filter_params=filter_params, fields=fields
            )

        count_query = f"""SELECT count() FROM ({query}) AS c_t"""

        return await self.client.fetchval(count_query)

    async def raw(self, query: str, command: str = "fetch", **kwargs) -> Any:

        commands = ["fetch", "fetchval", "execute", "fetchrow", "iterate"]

        assert command in commands, "it isn't accepted command"

        method = getattr(self.client, command)

        return await method(query)
