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
        ordering: Optional[List[str]] = None,
    ):
        """

        :param action: can be "select" or "insert"
        :param table: name of table in database
        :param db: name database in clickhouse
        :param values: data which insert in table
        :param filter_params: conditions for "where" block
        :param pagination: settings for pagination LIMIT OFFSET
        :param fields: name fields which use in select
        :param ordering: ORDER_BY settings
        """
        self.values = values
        self.filter_params = filter_params
        self.ordering = ordering
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
        return f"WHERE {where_str}"

    @staticmethod
    def ordering_string(ordering: List[str]) -> str:
        ordering_list = []
        for item in ordering:
            if len(item) > 0 and item[0] == "-":
                ordering_list.append(item[1:] + " DESC")
            elif len(item) > 0:
                ordering_list.append(item)

        ordering_string = ", ".join(ordering_list)
        return f"ORDER BY {ordering_string}"

    def _build(self) -> str:
        return getattr(self, f"_make_{self.action}_query")()

    def _make_insert_query(self):
        val_str = rows2ch(*self.values).decode()

        if self.fields:
            fields = ", ".join(self.fields)
            return f"INSERT INTO {self.db}.{self.table} ({fields}) VALUES {val_str}"

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
            pagination_string = f"LIMIT {limit} OFFSET {offset}"
        else:
            pagination_string = ""

        if self.ordering:
            ordering_string = self.ordering_string(self.ordering)
        else:
            ordering_string = ""

        result = filter(
            lambda item: item,
            [
                f"{select_str} FROM {self.db}.{self.table}",
                where_string,
                pagination_string,
                ordering_string,
            ],
        )
        return " ".join(result)


class BaseSQLBuilder(SQLBuilder):
    """
    Usage

    select_query = BaseSQLBuilder.select(destination, filter_params, pagination, fields, ordering)
    insert_query = BaseSQLBuilder.insert(destination, values)
    """

    @classmethod
    def insert(
        cls,
        destination: Tuple[str, str],
        values: List[tuple],
        fields: Optional[List[str]] = None,
    ) -> str:
        action = "insert"
        return cls(
            action, destination[1], destination[0], values, fields=fields
        )._build()

    @classmethod
    def select(
        cls,
        destination: Tuple[str, str],
        filter_params: Optional[dict] = None,
        pagination: Optional[dict] = None,
        fields: Optional[List[str]] = None,
        ordering: Optional[List[str]] = None,
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
            ordering=ordering,
        )._build()
