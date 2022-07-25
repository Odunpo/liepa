

destination_setup_ddls = [
    """
        CREATE TABLE sandbox.transactions_denormalized (
        id INT(11) UNSIGNED NOT NULL,
        dt TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        idoper TINYINT(4) NOT NULL,
        move TINYINT(4) NOT NULL,
        amount DECIMAL(10, 2) NOT NULL,
        name_oper VARCHAR(50) NOT NULL,
        PRIMARY KEY (id)
        )
        ENGINE = INNODB
    """,
]

destination_setup_data = [

]


destination_teardown_ddls = [
    "DROP TABLE IF EXISTS sandbox.transactions_denormalized",
]
