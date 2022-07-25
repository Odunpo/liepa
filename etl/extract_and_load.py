from datetime import datetime
from typing import Union
from pymysql.connections import Connection


def extract(conn: Connection, next_dt: datetime) -> Union[tuple, None]:
    """
    Returns data in transformed form
    conn
        source database connection object
    next_dt
        next datetime to extract in
    """
    with conn.cursor() as cursor:
        query = f"""
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
            where date(tr.dt) = date('{next_dt}')
            and hour(tr.dt) = hour('{next_dt}')
        """
        cursor.execute(query)
        if cursor.rowcount > 0:
            return cursor.fetchall()


def load(conn: Connection, data: Union[tuple, None]) -> None:
    """
    Loads data into destination database
    conn
        destination database connection object
    data
        data to load
    """
    if data:
        with conn.cursor() as cursor:
            query = """
                insert into transactions_denormalized
                values (%s, %s, %s, %s, %s, %s)
            """
            cursor.executemany(query, data)
