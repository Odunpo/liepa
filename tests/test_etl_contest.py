import pymysql as pm
import pymysql.cursors as pc


def test_containers_assets_is_ready(mysql_source,
                                    mysql_destination):

    with pm.connect(**mysql_source) as conn:
        with conn.cursor(pc.DictCursor) as cur:
            src_query = """
                SELECT 
                    COUNT(*) AS total 
                FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
            """

            cur.execute(src_query)
            src_result = cur.fetchone()

    with pm.connect(**mysql_destination) as conn:
        with conn.cursor(pc.DictCursor) as c:
            dst_query = """
                SELECT 
                    COUNT(*) AS total 
                FROM transactions_denormalized t
            """

            c.execute(dst_query)
            dst_result = c.fetchone()

    assert src_result['total'] > 0
    assert dst_result['total'] == 0


def test_data_transfer(mysql_source,
                       mysql_destination):
    """

    :param mysql_source: Доступы к mysql-источника с исходными данными
    :param mysql_destination: Доступы к mysql-назначения
    :return:
    """

    #   put your code for testing here!

    pass
