import pytest

from lib.ConfigReader import get_app_config
from lib.DataManipulation import count_orders_state, filter_closed_orders, filter_orders_generic
from lib.DataReader import read_customers, read_orders


@pytest.mark.transformations()
def test_read_customers(spark):
    customers_count = read_customers(spark,"LOCAL").count()
    assert customers_count == 12435


@pytest.mark.transformations()
def test_read_orders(spark):
    customers_count = read_orders(spark,"LOCAL").count()
    assert customers_count == 68884


@pytest.mark.transformations()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556

@pytest.mark.slow()
def test_read_app_config(spark):
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"


@pytest.mark.skip()
def test_count_orders_state(spark,expected_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_results.collect()




@pytest.mark.parametrize(
"status,count",
[("CLOSED", 7556),
("PENDING_PAYMENT", 15030),
("COMPLETE", 22899)])
def test_check_count_df(spark, status, count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df,status).count()
    assert filtered_count == count