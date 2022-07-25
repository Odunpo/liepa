from random import choice
from datetime import datetime, timedelta
from helpers import dst_connection, src_connection


def truncate_dst():
    """
    Truncates table transactions_denormalized
    in destination database
    """
    conn = dst_connection()
    with conn.cursor() as cur:
        cur.execute("truncate table transactions_denormalized")
    conn.close()


def delete_from_dst():
    """
    Deletes almost half of data in destination database
    in order to simulate ETL proccess interruption
    """
    conn = dst_connection()
    with conn.cursor() as cur:
        cur.execute("select round(max(id)/2) from transactions_denormalized")
        id_del_from = cur.fetchall()[0][0]
        query = f"""
            delete from transactions_denormalized
            where id >= {id_del_from}
        """
        cur.execute(query)
    conn.close()


def compare_rows():
    """
    Compares data in source and destination databases.
    Returns True if the data is the same
    """
    source_conn = src_connection()
    with source_conn.cursor() as cur:
        query = """
            select
                tr.id,
                tr.dt,
                tr.idoper,
                tr.move,
                tr.amount,
                op.name
            from transactions tr
            left join operation_types op
                on tr.idoper = op.id
            order by tr.id
        """
        cur.execute(query)
        source_data = cur.fetchall()
    source_conn.close()

    dest_conn = dst_connection()
    with dest_conn.cursor() as cur:
        query = "select * from transactions_denormalized order by id"
        cur.execute(query)
        dest_data = cur.fetchall()
    dest_conn.close()

    return source_data == dest_data


def scr_add_more_data():
    """
    Truncates transactions table in source database
    and creates more data with 5 years lags to test
    extract_hard type of getting next datetime from source database
    """
    values = []
    idopers = [1, 2, 3]
    moves = [-1, 1]
    amounts = [100, 150, 200, 250, 300]
    dates = [datetime(2010, 1, 1, 0), datetime(2015, 1, 1, 0), datetime(2022, 1, 1, 0)]
    for date in dates:
        for i in range(100):
            value = (
                date + timedelta(hours=i, minutes=choice([0, 20, 30, 50]), seconds=0),
                choice(idopers),
                choice(moves),
                choice(amounts),
            )
            values.append(value)
    conn = src_connection()
    with conn.cursor() as cur:
        cur.execute("truncate table transactions")
        base_query = """
            insert into transactions (dt, idoper, move, amount)
            values (%s, %s, %s, %s)
        """
        cur.executemany(base_query, values)
    conn.close()
