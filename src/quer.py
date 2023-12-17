import pandas

querSQl = [
    """
    SELECT "VendorID", COUNT(*)
    FROM trips GROUP BY 1;
    """,
    """
    SELECT "passenger_count", AVG("total_amount")
    FROM trips GROUP BY 1;
    """,
    """
    SELECT "passenger_count", EXTRACT(year FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)), COUNT(*)
    FROM trips GROUP BY 1, 2;
    """,
    """
    SELECT "passenger_count", EXTRACT(year FROM CAST("tpep_pickup_datetime" AS TIMESTAMP)), ROUND("trip_distance"), COUNT(*)
    FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;
    """
]

sqlite_quer = [
    """
    SELECT "VendorID", COUNT(*)
    FROM trips GROUP BY 1;
    """,
    """
    SELECT "passenger_count", AVG("total_amount")
    FROM trips GROUP BY 1;
    """,
    """
    SELECT "passenger_count", strftime('%Y', "tpep_pickup_datetime"), COUNT(*)
    FROM trips GROUP BY 1, 2;
    """,
    """
    SELECT "passenger_count", strftime('%Y', "tpep_pickup_datetime"), ROUND("trip_distance"), COUNT(*)
    FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 DESC;
    """
]

def group_by_vendor_id(fp):
    return fp.groupby("VendorID").size()
def mean_total_amount_by_passenger_count(fp):
    return fp.groupby("passenger_count")["total_amount"].mean()
def count_by_passenger_and_year(fp):
    fp['year'] = pandas.to_datetime(fp["tpep_pickup_datetime"]).dt.year
    return fp.groupby(["passenger_count", "year"]).size()
def count_by_passenger_year_and_distance(fp):
    fp['year'] = pandas.to_datetime(fp["tpep_pickup_datetime"]).dt.year
    fp['distance'] = fp["trip_distance"].round()
    return fp.groupby(["passenger_count", "year", "distance"]).size().sort_values(ascending=False)

pandas_quer = [
    group_by_vendor_id,
    mean_total_amount_by_passenger_count,
    count_by_passenger_and_year,
    count_by_passenger_year_and_distance
]
