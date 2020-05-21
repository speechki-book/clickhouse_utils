from clickhouse_utils.query_builder import BaseSQLBuilder
import pytest
import datetime as dt


destination = ("test_db", "test_table")
eq_error_msg = "queries not equal"


def test_insert():

    values = [
        (1, (dt.date(2018, 9, 7), None)),
        (2, (dt.date(2018, 9, 8), 3.14)),
    ]

    check_insert = "INSERT INTO test_db.test_table VALUES (1,('2018-09-07',NULL)),(2,('2018-09-08',3.14))"

    insert_query = BaseSQLBuilder.insert(destination, values)

    assert insert_query == check_insert, eq_error_msg


def test_simple_select():
    select_query = BaseSQLBuilder.select(destination)

    check_select = "SELECT * FROM test_db.test_table"

    assert select_query == check_select, eq_error_msg


def test_where_select():
    now = dt.datetime.now()
    str_now = now.strftime("%Y-%m-%d %H:%M:%S")
    conditions = {
        "a__exact": "ex_test",
        "b": "exact_s",
        "c__lt": 1,
        "v__lte": 2,
        "n__gt": now,
        "m__gte": now,
    }
    select_query = BaseSQLBuilder.select(destination, conditions)

    check_select = f"""SELECT * FROM test_db.test_table WHERE (a = 'ex_test') and (b = 'exact_s') and """\
        f"""(c < 1) and (v <= 2) and (n > '{str_now}') and (m >= '{str_now}')"""

    assert select_query == check_select, eq_error_msg


def test_pagination_select():
    limit = 100
    offset = 0

    select_query = BaseSQLBuilder.select(destination, pagination={"limit": limit, "offset": offset})

    check_select = f"SELECT * FROM test_db.test_table LIMIT {limit} OFFSET {offset}"

    assert select_query == check_select, eq_error_msg


def test_field_select():
    fields = ["a", "b", "v", "n", "m"]
    fields_str = ", ".join(fields)

    select_query = BaseSQLBuilder.select(destination, fields=fields)

    check_select = f"SELECT {fields_str} FROM test_db.test_table"

    assert select_query == check_select, eq_error_msg


def test_select():
    fields = ["a", "b", "v", "n", "m"]
    fields_str = ", ".join(fields)

    now = dt.datetime.now()
    str_now = now.strftime("%Y-%m-%d %H:%M:%S")
    conditions = {
        "a__exact": "ex_test",
        "b": "exact_s",
        "c__lt": 1,
        "v__lte": 2,
        "n__gt": now,
        "m__gte": now,
    }

    limit = 100
    offset = 0

    select_query = BaseSQLBuilder.select(destination, filter_params=conditions, fields=fields,
                                         pagination={"limit": limit, "offset": offset})

    check_select = f"""SELECT {fields_str} FROM test_db.test_table WHERE (a = 'ex_test') and (b = 'exact_s') and """ \
                   f"""(c < 1) and (v <= 2) and (n > '{str_now}') and (m >= '{str_now}') """\
                   f"LIMIT {limit} OFFSET {offset}"

    assert select_query == check_select, eq_error_msg
