from extract_and_load import extract, load
from helpers import (
    src_connection,
    dst_connection,
    src_next_dt,
    dst_last_dt,
    hard_extract_generator,
    hard_extract_manager,
)


def runner():
    """
    Runs ETL
    """
    source_conn = src_connection()
    dest_conn = dst_connection()
    extract_gen = hard_extract_generator()
    last_dt = dst_last_dt(dest_conn)
    next_dt = src_next_dt(source_conn, last_dt, False)
    while next_dt:
        data = extract(source_conn, next_dt)
        if data:
            extract_gen = hard_extract_generator()
        hard_extract = hard_extract_manager(data, extract_gen)
        load(dest_conn, data)
        last_dt = next_dt
        next_dt = src_next_dt(source_conn, last_dt, hard_extract)
    source_conn.close()
    dest_conn.close()
