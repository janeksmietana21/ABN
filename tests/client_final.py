from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

def rename_col(data, old_to_new_names):
    for old_name, new_name in old_to_new_names.items():
        data = data.withColumnRenamed(old_name, new_name)
    return data

def filter_data(data, countries):
    return data.filter((data.country.isin(countries)))

def process_data(data_one_path, data_two_path, countries_list):
    clients_df = spark.read.csv(data_one_path, header=True)
    finance_df = spark.read.csv(data_two_path, header=True)
    new_client_df = clients_df.drop('first_name', 'last_name')
    filtered_client_df = filter_data(new_client_df, countries_list)
    filtered_client_df = rename_col(filtered_client_df, {'id':'client_identifier'})

    new_finance_df = rename_col(finance_df, {'cc_t': 'credit_card_type', 'btc_a': 'bitcoin_address'})
    new_finance_df = new_finance_df.drop('cc_n')
    join_df = filtered_client_df.join(new_finance_df, filtered_client_df.client_identifier == finance_df.id, 'inner') \
        .drop('id')

    return join_df


join_df = process_data('test_one.csv', 'test_two.csv', ['United Kingdom', 'Netherlands'])
join_df.show(truncate=False)
