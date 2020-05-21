from typing import Optional, List, Dict, Any, Tuple, Union
from clickhouse_utils.sql.operators import OPERATORS
from clickhouse_utils.sql.mapper import py2ch, rows2ch


class SQLBuilder(object):
    def __init__(
        self,
        action: str,
        table: str,
        db: str,
        values: Optional[list] = None,
        filter_params: Optional[dict] = None,
        pagination: Optional[dict] = None,
        fields: Optional[List[str]] = None,
    ):
        """

        :param action: can be "select" or "insert"
        :param table: name of table in database
        :param db: name database in clickhouse
        :param values: data which insert in table
        :param filter_params: conditions for "where" block
        :param pagination: settings for pagination LIMIT OFFSET
        :param fields: name fields which use in select
        """
        self.values = values
        self.filter_params = filter_params
        self.pagination = pagination
        self.fields = fields
        self.action = action
        self.table = table
        self.db = db

    @staticmethod
    def _prepare_query_params(
        params: Optional[Dict[str, Any]] = None
    ) -> Union[Dict[str, Any], None]:
        """
        Escape symbols for use in SQL query. Defence versus SQL Injection

        :param params: raw data
        :return: data, which escape special symbols
        """
        if params is None:
            return params

        prepared_query_params = {}
        for key, value in params.items():
            prepared_query_params[key] = py2ch(value).decode("utf-8")
        return prepared_query_params

    @staticmethod
    def where_sting(filter_values: dict) -> str:
        """
        Support next operators:
         exact - =
         lt - <
         gt - >
         lte - <=
         gte - >=

        :param filter_values: dict with conditions. Key - field name and operator, Value - condition
        :return: complete condition string
        """
        if not filter_values:
            return ""

        conditions = []
        for key, value in filter_values.items():
            splited = key.split("__")
            if len(splited) < 2:
                operator = "exact"
                field_name = splited[0]
            else:
                operator = splited[1]
                field_name = splited[0]

            conditions.append(OPERATORS[operator].to_sql(field_name, value))

        where_str = " and ".join(conditions)
        return f" WHERE {where_str}"

    def _build(self) -> str:
        return getattr(self, f"_make_{self.action}_query")()

    def _make_insert_query(self):
        val_str = rows2ch(*self.values).decode()

        return f"INSERT INTO {self.db}.{self.table} VALUES {val_str}"

    def _make_select_query(self):
        where_string = self.where_sting(self.filter_params)

        if not self.fields:
            fields_string = "*"
        else:
            fields_string = ", ".join(self.fields)

        select_str = f"SELECT {fields_string}"

        if self.pagination:
            limit = self.pagination.get("limit", 100)
            offset = self.pagination.get("offset", 0)
            pagination_string = f" LIMIT {limit} OFFSET {offset}"
        else:
            pagination_string = ""

        return (
            f"{select_str} FROM {self.db}.{self.table}{where_string}{pagination_string}"
        )


class BaseSQLBuilder(SQLBuilder):
    """
    Usage

    select_query = BaseSQLBuilder.select(destination, filter_params, pagination, fields)
    insert_query = BaseSQLBuilder.insert(destination, values)
    """

    @classmethod
    def insert(cls, destination: Tuple[str, str], values: List[tuple]) -> str:
        action = "insert"
        return cls(action, destination[1], destination[0], values)._build()

    @classmethod
    def select(
        cls,
        destination: Tuple[str, str],
        filter_params: Optional[dict] = None,
        pagination: Optional[dict] = None,
        fields: Optional[List[str]] = None,
    ) -> str:
        action = "select"
        filter_params = cls._prepare_query_params(filter_params)
        return cls(
            action,
            destination[1],
            destination[0],
            filter_params=filter_params,
            pagination=pagination,
            fields=fields,
        )._build()
