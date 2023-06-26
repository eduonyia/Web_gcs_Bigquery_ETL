# This is a sample Python script.
from helper_functions import time_of_day, day_of_week
from google.cloud import bigquery
from google.cloud import storage
import tempfile
import pandas as pd
import numpy as np
import os


def load_taxi_zone(bucket, bqclient, dataset_name, project_id):
    pass


def load_fhv(bucket, bqclient, dataset_name, project_id):
    service = 'fhv'
    """
          ETL process for loading Green taxi trip data into BigQuery.
          Downloads & transformations  PARQUET files from Google Cloud Storage and loads data into BigQuery table.
      """
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("dispatching_base_num", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("sr_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("affiliated_base_num", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hour_of_day", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_hour", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_day", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_timeofday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_timeofday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("day_of_week", "STRING", mode="NULLABLE")

    ]

    # Create the BigQuery table
    table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
    table = bigquery.Table(table_id, schema=schema)
    table = bqclient.create_table(table)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    # Initialize bucket
    bucket_name = bucket
    folder_name = "fhv"

    # Retrieve all blobs with a prefix matching the file.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs iterate in folder
    blobs = bucket.list_blobs(prefix=folder_name)  # delimiter=delimiter
    # /green/2019/1-12*.parquet

    print(f"about downloading files from {bucket_name}")

    for blob in blobs:
        print(blob.name)

        with tempfile.NamedTemporaryFile("w", suffix=".parquet") as tempdir:
            destination_uri = tempdir.name
            print(destination_uri)
            blob.download_to_filename(destination_uri)

            df = pd.read_parquet(destination_uri)
            print(df.head())
            print(df.columns.values)
            print(f"Before cleaning, the dataframe has {df.shape[0]} rows")

            # Performing transformations: We will do some data cleaning
            df = df.rename(
                index=str,
                columns={
                    "dispatching_base_num": "dispatching_base_num",
                    "pickup_datetime": "pickup_time",
                    "dropOff_datetime": "dropoff_time",
                    "PUlocationID": "pickup_location_id",
                    "DOlocationID": "dropoff_location_id",
                    "SR_Flag": "sr_flag",
                    "Affiliated_base_number": "affiliated_base_num",
                },
            )

            # Remove duplicates - first step is to drop generic duplicates
            df = df.drop_duplicates()
            # print(df.columns)
            df.dropna()

            print(f"After cleaning, the dataframe is left with {df.shape[0]} rows")

            # change to python string for conversion
            df['pickup_time'] = df['pickup_time'].astype('str')
            df['dropoff_time'] = df['dropoff_time'].astype('str')

            # Categorize trips into seasons
            conditions = [
                (df['pickup_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')) &
                (df['dropoff_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')),
                (df['pickup_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')) &
                (df['dropoff_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')),
                (df['pickup_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')) &
                (df['dropoff_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')),
                (df['pickup_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')) &
                (df['dropoff_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')),
                (df['pickup_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59')) &
                (df['dropoff_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59'))
            ]
            values = ['winter', 'spring', 'summer', 'autumn', 'winter_0']
            df['season'] = np.select(conditions, values)
            df['season'] = df['season'].replace(['winter_0'], 'winter')

            # change to pandas date time for conversion
            df['pickup_time'] = pd.to_datetime(df['pickup_time'])
            df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])

            df["hour_of_day"] = df["pickup_time"].dt.hour
            df["dropoff_hour"] = df["dropoff_time"].dt.hour
            df["pickup_day"] = df["pickup_time"].dt.dayofweek

            # Time of the day:  Call Time zones helper function
            df["pickup_timeofday"] = df["hour_of_day"].apply(time_of_day)
            df["dropoff_timeofday"] = df["dropoff_hour"].apply(time_of_day)

            #  Call Day of the week helper function
            df["day_of_week"] = df["pickup_day"].apply(day_of_week)

            # change columns to string
            df["hour_of_day"] = df["hour_of_day"].apply(str)
            df["dropoff_hour"] = df["dropoff_hour"].apply(str)
            df["pickup_day"] = df["pickup_day"].apply(str)
            df["pickup_timeofday"] = df["pickup_timeofday"].apply(str)
            df["dropoff_timeofday"] = df["dropoff_hour"].apply(str)
            df["day_of_week"] = df["day_of_week"].apply(str)
            df['affiliated_base_num'] = df['affiliated_base_num'].apply(str)
            df['sr_flag'] = df['sr_flag'].apply(str)
            df['dispatching_base_num'] = df['dispatching_base_num'].apply(str)
            # df['pickup_location_id'] = df['pickup_location_id'].apply(str)
            # df['dropoff_location_id'] = df['dropoff_location_id'].apply(str)

            # construct a job configuration
            table_id = f"{project_id}.{dataset_name}.{service}_tripdata"

            print("Started loading to BigQuery....")

            job_config = bigquery.LoadJobConfig(autodetect=False)

            # Submit the job
            job = bqclient.load_table_from_dataframe(df, table_id, job_config=job_config)

            # Wait for the job to complete and then show the job results
            job.result()

            # Read back the properties of the table
            table = bqclient.get_table(table_id)
            print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            print("JOB SUCCESSFUL")


def load_green(bucket, bqclient, dataset_name, project_id):
    service = 'green'
    """
           ETL process for loading Green taxi trip data into BigQuery.
           Downloads & transformations  PARQUET files from Google Cloud Storage and loads data into BigQuery table.
       """
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("vendor_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("store_and_fwd_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("rate_code_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("passengers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("distance", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("fare", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tip", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("payment_type", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("congestion_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hour_of_day", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_hour", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_day", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_timeofday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_timeofday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("day_of_week", "STRING", mode="NULLABLE")

    ]

    # Create the BigQuery table
    table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
    table = bigquery.Table(table_id, schema=schema)
    table = bqclient.create_table(table)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    # Initialize bucket
    bucket_name = bucket
    folder_name = "green"

    # Retrieve all blobs with a prefix matching the file.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs iterate in folder
    blobs = bucket.list_blobs(prefix=folder_name)  # delimiter=delimiter
    # /green/2019/1-12*.parquet

    print(f"about downloading files from {bucket_name}")

    for blob in blobs:
        print(blob.name)

        with tempfile.NamedTemporaryFile("w", suffix=".parquet") as tempdir:
            destination_uri = tempdir.name
            print(destination_uri)
            blob.download_to_filename(destination_uri)

            df = pd.read_parquet(destination_uri)
            print(df.head())
            print(df.columns.values)
            print(f"Before cleaning, the dataframe has {df.shape[0]} rows")

            # Performing transformations: We will do some data cleaning
            df = df.rename(index=str,
                           columns={'VendorID': 'vendor_id', \
                                    'lpep_pickup_datetime': 'pickup_time', \
                                    'lpep_dropoff_datetime': 'dropoff_time', \
                                    'passenger_count': 'passengers', \
                                    'RatecodeID': 'rate_code_id', \
                                    'PULocationID': 'pickup_location_id', \
                                    'DOLocationID': 'dropoff_location_id', \
                                    'trip_distance': 'distance', \
                                    'fare_amount': 'fare', \
                                    'tip_amount': 'tip'})

            # Remove duplicates - first step is to drop generic duplicates
            df = df.drop_duplicates()
            # print(df.columns)

            df = df.drop(['trip_type'], axis=1)
            df = df.drop(['ehail_fee'], axis=1)

            # Replacing missing rows in the rate_code_id column with the most common rate code ID
            df['rate_code_id'] = df['rate_code_id'].fillna(df['rate_code_id'].mode()[0])

            # Dropping all rows with zero in the distance, fare, total_amount and improvement charge column
            df = df[df['distance'] > 0]
            df = df[df['fare'] > 0]
            df.dropna()

            print(f"After cleaning, the dataframe is left with {df.shape[0]} rows")

            # change to python string for conversion
            df['pickup_time'] = df['pickup_time'].astype('str')
            df['dropoff_time'] = df['dropoff_time'].astype('str')

            # Categorize trips into seasons
            conditions = [
                (df['pickup_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')) &
                (df['dropoff_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')),
                (df['pickup_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')) &
                (df['dropoff_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')),
                (df['pickup_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')) &
                (df['dropoff_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')),
                (df['pickup_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')) &
                (df['dropoff_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')),
                (df['pickup_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59')) &
                (df['dropoff_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59'))
            ]
            values = ['winter', 'spring', 'summer', 'autumn', 'winter_0']
            df['season'] = np.select(conditions, values)
            df['season'] = df['season'].replace(['winter_0'], 'winter')

            # change to pandas date time for conversion
            df['pickup_time'] = pd.to_datetime(df['pickup_time'])
            df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])

            df["hour_of_day"] = df["pickup_time"].dt.hour
            df["dropoff_hour"] = df["dropoff_time"].dt.hour
            df["pickup_day"] = df["pickup_time"].dt.dayofweek

            # Time of the day:  Call Time zones helper function
            df["pickup_timeofday"] = df["hour_of_day"].apply(time_of_day)
            df["dropoff_timeofday"] = df["dropoff_hour"].apply(time_of_day)

            #  Call Day of the week helper function
            df["day_of_week"] = df["pickup_day"].apply(day_of_week)

            # change columns to string
            df["hour_of_day"] = df["hour_of_day"].apply(str)
            df["dropoff_hour"] = df["dropoff_hour"].apply(str)
            df["pickup_day"] = df["pickup_day"].apply(str)
            df["pickup_timeofday"] = df["pickup_timeofday"].apply(str)
            df["dropoff_timeofday"] = df["dropoff_hour"].apply(str)
            df["day_of_week"] = df["day_of_week"].apply(str)

            # construct a job configuration
            table_id = f"{project_id}.{dataset_name}.{service}_tripdata"

            print("Started loading to BigQuery....")

            job_config = bigquery.LoadJobConfig(autodetect=False)

            # Submit the job
            job = bqclient.load_table_from_dataframe(df, table_id, job_config=job_config)

            # Wait for the job to complete and then show the job results
            job.result()

            # Read back the properties of the table
            table = bqclient.get_table(table_id)
            print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            print("JOB SUCCESSFUL")


def load_yellow(bucket, bqclient, dataset_name, project_id):
    service = 'yellow'
    """
           ETL process for loading Green taxi trip data into BigQuery.
           Downloads & transformations  PARQUET files from Google Cloud Storage and loads data into BigQuery table.
       """
    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("vendor_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("pickup_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_time", "TIMESTAMP", mode="NULLABLE"),
        bigquery.SchemaField("passengers", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("distance", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("rate_code_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("sf_flag", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_location_id", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("payment_type", "INT64", mode="NULLABLE"),
        bigquery.SchemaField("fare", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("extra", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("mta_tax", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tip", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("tolls_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("improvement_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("total_amount", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("congestion_surcharge", "FLOAT64", mode="NULLABLE"),
        bigquery.SchemaField("season", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("hour_of_day", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_hour", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_day", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("pickup_timeofday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("dropoff_timeofday", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("day_of_week", "STRING", mode="NULLABLE")

    ]

    # Create the BigQuery table
    table_id = f"{project_id}.{dataset_name}.{service}_tripdata"
    table = bigquery.Table(table_id, schema=schema)
    table = bqclient.create_table(table)  # Make an API request.
    print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    # Initialize bucket
    bucket_name = bucket
    folder_name = "yellow"

    # Retrieve all blobs with a prefix matching the file.
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)

    # List blobs iterate in folder
    blobs = bucket.list_blobs(prefix=folder_name)  # delimiter=delimiter
    # /green/2019/1-12*.parquet

    print(f"about downloading files from {bucket_name}")

    for blob in blobs:
        print(blob.name)

        with tempfile.NamedTemporaryFile("w", suffix=".parquet") as tempdir:
            destination_uri = tempdir.name
            print(destination_uri)
            blob.download_to_filename(destination_uri)

            df = pd.read_parquet(destination_uri)
            print(df.head())
            print(df.columns.values)
            print(f"Before cleaning, the dataframe has {df.shape[0]} rows")

            # Performing transformations: We will do some data cleaning
            df = df.rename(index=str,
                           columns={'VendorID': 'vendor_id', \
                                    'tpep_pickup_datetime': 'pickup_time', \
                                    'tpep_dropoff_datetime': 'dropoff_time', \
                                    'passenger_count': 'passengers', \
                                    'RatecodeID': 'rate_code_id', \
                                    'store_and_fwd_flag': 'sf_flag', \
                                    'PULocationID': 'pickup_location_id', \
                                    'DOLocationID': 'dropoff_location_id', \
                                    'trip_distance': 'distance', \
                                    'fare_amount': 'fare', \
                                    'tip_amount': 'tip'})

            # Remove duplicates - first step is to drop generic duplicates
            df = df.drop_duplicates()
            # print(df.columns)

            # Replacing missing rows in the rate_code_id column with the most common rate code ID
            df['rate_code_id'] = df['rate_code_id'].fillna(df['rate_code_id'].mode()[0])

            # Dropping all rows with zero in the distance, fare, total_amount and improvement charge column
            df = df[df['distance'] > 0]
            df = df[df['fare'] > 0]
            df.dropna()

            print(f"After cleaning, the dataframe is left with {df.shape[0]} rows")

            # change to python string for conversion
            df['pickup_time'] = df['pickup_time'].astype('str')
            df['dropoff_time'] = df['dropoff_time'].astype('str')

            # Categorize trips into seasons
            conditions = [
                (df['pickup_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')) &
                (df['dropoff_time'].between('2018-12-01 00:00:00', '2019-02-28 23:59:59')),
                (df['pickup_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')) &
                (df['dropoff_time'].between('2019-03-01 00:00:00', '2019-05-31 23:59:59')),
                (df['pickup_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')) &
                (df['dropoff_time'].between('2019-06-01 00:00:00', '2019-08-31 23:59:59')),
                (df['pickup_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')) &
                (df['dropoff_time'].between('2019-09-01 00:00:00', '2019-11-30 23:59:59')),
                (df['pickup_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59')) &
                (df['dropoff_time'].between('2019-12-01 00:00:00', '2019-12-31 23:59:59'))
            ]
            values = ['winter', 'spring', 'summer', 'autumn', 'winter_0']
            df['season'] = np.select(conditions, values)
            df['season'] = df['season'].replace(['winter_0'], 'winter')

            # change to pandas date time for conversion
            df['pickup_time'] = pd.to_datetime(df['pickup_time'])
            df['dropoff_time'] = pd.to_datetime(df['dropoff_time'])

            df["hour_of_day"] = df["pickup_time"].dt.hour
            df["dropoff_hour"] = df["dropoff_time"].dt.hour
            df["pickup_day"] = df["pickup_time"].dt.dayofweek

            # Time of the day:  Call Time zones helper function
            df["pickup_timeofday"] = df["hour_of_day"].apply(time_of_day)
            df["dropoff_timeofday"] = df["dropoff_hour"].apply(time_of_day)

            #  Call Day of the week helper function
            df["day_of_week"] = df["pickup_day"].apply(day_of_week)

            # change columns to string
            df["hour_of_day"] = df["hour_of_day"].apply(str)
            df["dropoff_hour"] = df["dropoff_hour"].apply(str)
            df["pickup_day"] = df["pickup_day"].apply(str)
            df["pickup_timeofday"] = df["pickup_timeofday"].apply(str)
            df["dropoff_timeofday"] = df["dropoff_hour"].apply(str)
            df["day_of_week"] = df["day_of_week"].apply(str)

            # construct a job configuration
            table_id = f"{project_id}.{dataset_name}.{service}_tripdata"

            print("Started loading to BigQuery....")

            job_config = bigquery.LoadJobConfig(autodetect=False)

            # Submit the job
            job = bqclient.load_table_from_dataframe(df, table_id, job_config=job_config)

            # Wait for the job to complete and then show the job results
            job.result()

            # Read back the properties of the table
            table = bqclient.get_table(table_id)
            print("Table:", table.table_id, "has", table.num_rows, "rows and", len(table.schema), "columns")
            print("JOB SUCCESSFUL")


def load_service(service, bucket, bqclient, dataset_name, project_id):
    if service == 'taxi':
        load_taxi_zone(bucket, bqclient, dataset_name, project_id)
    elif service == 'fhv':
        load_fhv(bucket, bqclient, dataset_name, project_id)
    elif service == 'green':
        load_green(bucket, bqclient, dataset_name, project_id)
    elif service == 'yellow':
        load_yellow(bucket, bqclient, dataset_name, project_id)


def main():
    print("Program started...")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'Bigquery_Edu_Service.json'
    bucket = os.getenv("GCP_GCS_bucket", "my-gcs-bucket1")
    dataset_name = "NYC"
    project_id = "leafy-stock-390108"

    # Construct a BigQuery client object.
    bqclient = bigquery.Client()

    services = ['taxi']
    # services = ['taxi', 'fhv', 'green', 'yellow']
    for service in services:
        load_service(service, bucket, bqclient, dataset_name, project_id)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
