from runner import runner
from tools_for_tests import (
    truncate_dst,
    delete_from_dst,
    compare_rows,
    scr_add_more_data,
)


def test_first_case():

    # truncating table transactions_denormalized
    truncate_dst()

    # is source data equal to destination data? (No)
    first_comparison = compare_rows()

    # running ETL
    runner()

    # is source data equal to destination data? (Yes)
    second_comparison = compare_rows()

    assert (first_comparison, second_comparison) == (False, True)


def test_second_case():

    # deleting part of data in destination db
    # in order to simulate ETL interruption
    delete_from_dst()

    # is source data equal to destination data? (No)
    first_comparison = compare_rows()

    # running ETL
    runner()

    # is source data equal to destination data? (Yes)
    second_comparison = compare_rows()

    assert (first_comparison, second_comparison) == (False, True)


def test_third_case():

    # for case when source data is equal to
    # destination data before runner starts

    # is source data equal to destination data? (Yes)
    first_comparison = compare_rows()

    # running ETL
    runner()

    # is source data equal to destination data? (Yes)
    second_comparison = compare_rows()

    assert (first_comparison, second_comparison) == (True, True)


def test_fourth_case():

    # case with more data in source table
    # where we have big lags in data (about 5 years)

    # truncating table transactions and adding more data
    scr_add_more_data()

    # truncating table transactions_denormalized
    truncate_dst()

    # is source data equal to destination data? (No)
    first_comparison = compare_rows()

    # running ETL
    runner()

    # is source data equal to destination data? (Yes)
    second_comparison = compare_rows()

    assert (first_comparison, second_comparison) == (False, True)


def test_fifth_case():

    # case with more data in source table
    # where we have big lags in data (about 5 years)

    # deleting part of data in destination db
    # in order to simulate the situation
    # of ETL interruption
    delete_from_dst()

    # is source data equal to destination data? (No)
    first_comparison = compare_rows()

    # running ETL
    runner()

    # is source data equal to destination data? (Yes)
    second_comparison = compare_rows()

    assert (first_comparison, second_comparison) == (False, True)


def test_sixth_case():

    # for case when source data is equal to
    # destination data before runner starts

    # is source data equal to destination data? (Yes)
    first_comparison = compare_rows()

    # running ETL
    runner()

    # is source data equal to destination data? (Yes)
    second_comparison = compare_rows()

    assert (first_comparison, second_comparison) == (True, True)
