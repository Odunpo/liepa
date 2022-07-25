import datetime


source_setup_ddls = [
    """
        CREATE TABLE sandbox.transactions (
          id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
          dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          idoper TINYINT(4) NOT NULL,
          move TINYINT(4) NOT NULL,
          amount DECIMAL(10, 2) NOT NULL,
          PRIMARY KEY (id)
        )
        ENGINE = INNODB
    """,
    """
        CREATE TABLE sandbox.operation_types (
          id INT(11) UNSIGNED NOT NULL AUTO_INCREMENT,
          name VARCHAR(50) NOT NULL,
          PRIMARY KEY (id)
        )
        ENGINE = INNODB
    """
]

_source_transactions_data = [
    (datetime.datetime(2020, 1, 1, 0, 0, 0), 1, -1, 100),
    (datetime.datetime(2020, 1, 1, 1, 0, 0), 1, -1, 100),
    (datetime.datetime(2020, 1, 1, 2, 0, 0), 2, -1, 100),
    (datetime.datetime(2020, 1, 1, 2, 0, 0), 2, -1, 100),
    (datetime.datetime(2020, 1, 1, 3, 0, 0), 3, 1, 150),
    (datetime.datetime(2020, 1, 1, 3, 0, 0), 3, 1, 200)
]

_source_operation_types_data = [
    (1, "Subscription purchase"),
    (2, "Subscription update"),
    (3, "Account deposit")
]

source_setup_data = [
    [
        """
            INSERT INTO transactions (dt, idoper, move, amount) 
                VALUES (%s, %s, %s, %s)
        """,
        _source_transactions_data
    ],
    [
        """
            INSERT INTO operation_types (id, name) 
                VALUES (%s, %s)
        """,
        _source_operation_types_data
    ]
]

source_teardown_ddls = [
    "DROP TABLE IF EXISTS sandbox.transactions",
    "DROP TABLE IF EXISTS sandbox.operation_types"
]
