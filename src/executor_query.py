import statistics
import time
from sqlalchemy import text

def execute_query(connect, cursor, q):
    times = []
    for i in range(10):
        start = time.perf_counter()

        if hasattr(cursor, 'groups') and callable(getattr(cursor, 'groups')):
            q(cursor)
        elif hasattr(connect, 'connect') and callable(getattr(connect, 'connect')):
            with connect.connect() as connection:
                result = connection.execute(text(q))
                result.fetchall()

        else:
            cursor.execute(q)
            cursor.fetchall()

        end = time.perf_counter()
        times.append(end - start)
    return statistics.mean(times)


def execute_queries(connect, cursor, qs):
    res = []
    for q in qs:
        res.append(execute_query(connect, cursor, q))
    return res

def ress(res):
    return [round(num, 3) for num in res]