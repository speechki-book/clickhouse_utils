class Operator(object):
    def to_sql(self, field_name: str, value):
        raise NotImplementedError


class SimpleOperator(Operator):
    def __init__(self, sql_operator, sql_for_null=None):
        self._sql_operator = sql_operator
        self._sql_for_null = sql_for_null

    def to_sql(self, field_name, value):

        return f"({field_name} {self._sql_operator} {value})"


OPERATORS = {}


def register_operator(name, operator_class: Operator):
    OPERATORS[name] = operator_class


register_operator("exact", SimpleOperator("="))
register_operator("lt", SimpleOperator("<"))
register_operator("lte", SimpleOperator("<="))
register_operator("gt", SimpleOperator(">"))
register_operator("gte", SimpleOperator(">="))
