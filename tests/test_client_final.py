from pyspark.sql import SparkSession
import logging

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import SparkSession
from client_final import process_data


@pytest.fixture(scope='session')
def spark():
    return SparkSession.builder \
        .master("local") \
        .appName("chispa") \
        .getOrCreate()

spark = SparkSession.builder.appName("DataFrame").master('local').getOrCreate()

def test_process_data(spark):
    # client_data = [
    #     (32, 'Wallis', 'Bamford', 'wbamfordv@t-online.de', 'United Kingdom'),
    #     (36, 'Daniel', 'Buckthorp', 'dbuckthorpz@tmall.com', 'Netherlands'),
    #     (39, 'Imogene', 'Mascall', 'imascall12@networkadvertising.org', 'United States')]
    #
    # client_df = spark.createDataFrame(client_data, ['id', 'first_name', 'last_name', 'email', 'country'])
    #
    # finance_data = [(32, '12sxmYnPcADAXw1YkxdapRsft2PwHZke7A', 'maestro', '50387077934280351'),
    #                 (36, '15X53Z9B9jUNrvFpbr7D554uSc5RL7Pnkg', 'diners-club-international', '36423633151016'),
    #                 (39, '1HTwtuDq4sujd4RVZ4RbfVY4j3A31wyuNy', 'jcb', '3553603785705092')]
    #
    # finance_df = spark.createDataFrame(finance_data, ['id', 'btc_a', 'cc_t', 'cc_n'])

    actual_df = process_data('test_one.csv', 'test_two.csv', ['United Kingdom', 'Netherlands'])

    expected_data = [
        ('32', 'wbamfordv@t-online.de', 'United Kingdom', '12sxmYnPcADAXw1YkxdapRsft2PwHZke7A', 'maestro'),
        ('36', 'dbuckthorpz@tmall.com', 'Netherlands', '15X53Z9B9jUNrvFpbr7D554uSc5RL7Pnkg', 'diners-club-international')
    ]

    expected_df = spark.createDataFrame(expected_data, ['client_identifier', 'email', 'country', 'bitcoin_address', 'credit_card_type'])
    assert_df_equality(actual_df, expected_df)

test_process_data(spark)
