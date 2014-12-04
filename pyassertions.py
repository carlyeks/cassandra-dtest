import re
from cassandra import InvalidRequest, Unavailable, ConsistencyLevel, WriteTimeout, ReadTimeout
from cassandra.query import SimpleStatement
from pytools import rows_to_list

def assert_unavailable(fun, *args):
    try:
        if len(args) == 0:
            fun(None)
        else:
            fun(*args)
    except (Unavailable, WriteTimeout, ReadTimeout) as e:
        pass
    except Exception as e:
        assert False, "Expecting unavailable exception, got: " + str(e)
    else:
        assert False, "Expecting unavailable exception but no exception was raised"

def assert_invalid(session, query, matching=None, expected=InvalidRequest):
    try:
        res = session.execute(query)
        assert False, "Expecting query to be invalid: got %s" % res
    except AssertionError as e:
        raise e
    except expected as e:
        msg = str(e)
        if matching is not None:
            assert re.search(matching, msg), "Error message does not contain " + matching + " (error = " + msg + ")"

def assert_one(cursor, query, expected, cl=ConsistencyLevel.ONE):
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = cursor.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [expected], "Expected %s from %s, but got %s" % (expected, query, list_res)

def assert_none(cursor, query, cl=ConsistencyLevel.ONE):
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = cursor.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == [], "Expected nothing from %s, but got %s" % (query, list_res)

def assert_all(cursor, query, expected, cl=ConsistencyLevel.ONE):
    simple_query = SimpleStatement(query, consistency_level=cl)
    res = cursor.execute(simple_query)
    list_res = rows_to_list(res)
    assert list_res == expected, "Expected %s from %s, but got %s" % (expected, query, list_res)

def assert_almost_equal(*args, **kwargs):
    try:
        error = kwargs['error']
    except KeyError:
        error = 0.16

    vmax = max(args)
    vmin = min(args)
    assert vmin > vmax * (1.0 - error), "values not within %.2f%% of the max: %s" % (error * 100, args)
