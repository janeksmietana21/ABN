from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame").getOrCreate()


def filter_data(data_one_path, data_two_path, countries_list):
    clients_df = spark.read.csv(data_one_path, header=True)
    finance_df = spark.read.csv(data_two_path, header=True)
    filtered_client_df = clients_df.drop('first_name', 'last_name') \
        .filter((clients_df.country.isin(countries_list))) \
        .withColumnRenamed('id', 'client_identifier')

    finance_df = finance_df.withColumnRenamed('btc_a', 'bitcoin_address') \
        .withColumnRenamed('cc_t', 'credit_card_type') \
        .drop('cc_n')
    join_df = filtered_client_df.join(finance_df, filtered_client_df.client_identifier == finance_df.id, 'inner')\
        .drop('id')
    return join_df


join_df = filter_data('dataset_one.csv', 'dataset_two.csv', ['United Kingdom', 'Netherlands'])
join_df.show(truncate=False)

