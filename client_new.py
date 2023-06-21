from pyspark.sql import SparkSession
import logging

logging.basicConfig(filename='status.log', encoding='utf-8', level=logging.DEBUG)

spark = SparkSession.builder.appName("DataFrame").getOrCreate()


def rename_col(data, old_to_new_names):
    """
    
    :param data: Dataframe
    :param old_to_new_names: Dictionary which keys are old names and the values are new names
    :return: Pyspark dataframe that renamed the name of columns of data with new names
    """
    for old_name, new_name in old_to_new_names.items():
        data = data.withColumnRenamed(old_name, new_name)
    return data


def filter_data(data, countries):
    """
    
    :param data: Dataframe  
    :param countries: list 
    :return: pysapark dataframe with countries that were specified in a list 
    """
    return data.filter((data.country.isin(countries)))


def process_data(data_one_path, data_two_path, countries_list):
    """
    
    :param data_one_path: csv file
    :param data_two_path: csv file
    :param countries_list: list 
    :return: marged datframe after filtering and renaming columns 
    """
    clients_df = spark.read.csv(data_one_path, header=True)
    finance_df = spark.read.csv(data_two_path, header=True)
    new_client_df = clients_df.drop('first_name', 'last_name')
    filtered_client_df = filter_data(new_client_df, countries_list)
    filtered_client_df = rename_col(filtered_client_df, {'id':'client_identifier'})
    logging.info('client filtered by countries')

    new_finance_df = rename_col(finance_df, {'cc_t': 'credit_card_type', 'btc_a': 'bitcoin_address'})
    logging.info('rename columns')
    new_finance_df = new_finance_df.drop('cc_n')
    join_df = filtered_client_df.join(new_finance_df, filtered_client_df.client_identifier == finance_df.id, 'inner') \
        .drop('id')
    logging.info('Join Done!')
    join_df.write.format("csv").mode('overwrite').save("/tmp/spark_output/zipcodes")

    return join_df


join_df = process_data('dataset_one.csv', 'dataset_two.csv', ['United Kingdom', 'Netherlands'])
join_df.show(truncate=False)