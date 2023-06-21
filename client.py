# Import PySpark modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Import logging module
import logging

# Import chispa for Spark tests
import chispa

# Import Click for command-line interface
import click


# Set the logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Create a SparkSession
spark = SparkSession.builder.appName('KommatiPara').getOrCreate()

# Define the helper function to filter data by country
def filter_by_country(df, countries):
    filtered_df = df.filter(col('country').isin(countries))
    return filtered_df

# Define the helper function to rename columns
def rename_columns(df, old_to_new_names):
    renamed_df = df.toDF(*[old_to_new_names.get(c, c) for c in df.columns])
    return renamed_df

# Define the function to process the client data
def process_client_data(dataset_one_path, dataset_two_path, countries):
    # Load the client datasets
    dataset_one = spark.read.csv(dataset_one_path, header=True)
    dataset_two = spark.read.csv(dataset_two_path, header=True)
    logging.info('loading dataset')
    # Filter data by country
    dataset_one_filtered = filter_by_country(dataset_one, countries)
    
    # Remove personal identifiable information from dataset_one (excluding emails)
    dataset_one_filtered = dataset_one_filtered.drop('name', 'address', 'phone')
    
    # Remove credit card number from dataset_two
    dataset_two = dataset_two.drop('credit_card_number')
    
    # Join the datasets using the id field
    joined_df = dataset_one_filtered.join(dataset_two, 'id', 'inner')
    
    # Rename columns
    column_mapping = {'id': 'client_identifier', 'btc_a': 'bitcoin_address', 'cc_t': 'credit_card_type'}
    joined_df_renamed = rename_columns(joined_df, column_mapping)
    
    return joined_df_renamed

# Run Spark tests using chispa
def run_tests():
    dataset_one_path = "dataset_one.csv"
    dataset_two_path = "dataset_two.csv"
    countries = ["United Kingdom", "Netherlands"]
    
    result_df = process_client_data(dataset_one_path, dataset_two_path, countries)
    
    # Define the expected result
    expected_result = spark.createDataFrame(
        [("1", "btc123", "Visa")],
        ["client_identifier", "bitcoin_address", "credit_card_type"]
    )
    
    # Use chispa to perform the test
    chispa.assert_df_equality(result_df, expected_result)

@click.command()
@click.option('--dataset_one_path', type=click.Path(exists=True), help='Path to dataset one CSV file')
@click.option('--dataset_two_path', type=click.Path(exists=True), help='Path to dataset two CSV file')
@click.option('--countries', type=str, default="", help='Comma-separated list of countries to filter')
def process_data(dataset_one_path, dataset_two_path, countries):
    countries = countries.split(',') if countries else []
    
    result_df = process_client_data(dataset_one_path, dataset_two_path, countries)
    
    # Display the result
    result_df.show()

    # Log an info message
    logging.info('Starting the data processing...')

    # Save the resulting dataset in the client_data directory
    output_path = "result_dataset1.csv"
    result_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

    # Run tests
    run_tests()

if __name__ == "__main__":
    process_data()

