import json
import pathlib
from datetime import datetime, timedelta
from typing import Union
import pymysql
from pymysql.connections import Connection


def config_path() -> pathlib.Path:
    """
    Returns configuration path
    """
    here = pathlib.Path(__name__).resolve()
    config_path = here.parents[1] / "dbconf.json"
    return config_path


def db_creds(config_path: pathlib.Path, db_type: str) -> dict:
    """
    Returns database credentials for source/destination database
        config_path
            path to json config (see config_path func)
        db_type
            could be either "mysql_src" or "mysql_dst"
    """
    with open(config_path) as path:
        full_creds = json.load(path)
        return full_creds[db_type]


def db_connection(db_creds: dict) -> Connection:
    """
    Returns connection object for database
        db_creds
            database credentials (dict type)
    """
    return pymysql.connect(**db_creds, autocommit=True)


def src_connection() -> Connection:
    """
    Returns connection object for source database
    """
    conf_path = config_path()
    creds = db_creds(conf_path, "mysql_src")
    return db_connection(creds)


def dst_connection() -> Connection:
    """
    Returns connection object for destination database
    """
    conf_path = config_path()
    creds = db_creds(conf_path, "mysql_dst")
    return db_connection(creds)


def dst_last_dt(conn: Connection) -> Union[datetime, None]:
    """
    Returns last datetime from destination database
        conn
            connection object for destination database
    """
    with conn.cursor() as cursor:
        query = """
            select max(dt)
            from transactions_denormalized
        """
        cursor.execute(query)
        if cursor.rowcount > 0:
            return cursor.fetchall()[0][0]


def src_next_dt(
    conn: Connection, last_dt: Union[datetime, None], hard_extract: bool
) -> Union[datetime, None]:
    """
    Returns next datetime to extract from source database
        conn
            connection object for source database
        last_dt
            last datetime from destination database
        hard_extract
            True for searching next datetime in source database
            False for incrementing last datetime
    """
    if not last_dt or hard_extract:
        query = "select min(dt) from transactions"
        if last_dt:
            # cond = f"\nwhere date(dt) >= date('{last_dt}')\nand hour(dt) > hour('{last_dt}')"
            cond = f'\nwhere date_format(dt, "%Y-%m-%d %H:00:00") > date_format("{last_dt}", "%Y-%m-%d %H:00:00")'
            query = query + cond
        with conn.cursor() as cur:
            cur.execute(query)
            if cur.rowcount > 0:
                return cur.fetchall()[0][0]
    else:
        return last_dt + timedelta(hours=1)


def hard_extract_generator(itter: int = 24) -> bool:
    """
    Returns bool flag for hard_extract_manager fucntion
        itter
            how many empty data batches pass before use hard extract
            for searching the next datetime in source database
            instead of incrementing
    """
    num = 1
    while True:
        if num <= itter:
            num += 1
            yield False
        else:
            num > itter
            num = 1
            yield True


def hard_extract_manager(data: Union[tuple, None], generator) -> bool:
    """
    Returns bool flag for hard_extract field for src_next_dt function
        data
            data from the last call of extract function
        generator
            object of hard_extract_generator
    """
    if not data:
        return next(generator)
    else:
        return False
