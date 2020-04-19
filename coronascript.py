# Importamos la librería bigquery
from google.cloud import bigquery
# Importamos la librería de cuenta de servicios de BQ
from google.oauth2 import service_account
import pandas as pd
import numpy as np

# Construct a BigQuery client object.
proj_ID = 'aischool-272715'
creds = service_account.Credentials.from_service_account_file('key/aischool-272715-b74ba1ac9e13.json')


def main():
  # BigQuery API
  client = bigquery.Client(credentials=creds, project=proj_ID)
  dataset_id = "{}.natal".format(client.project)
  # Construct a full Dataset object to send to the API.
  dataset = bigquery.Dataset(dataset_id)
  dataset.location = "US"
  dataset = client.create_dataset(dataset)  # Make an API request.
  # Configure the external data source and query job.
  external_config = bigquery.ExternalConfig("CSV")
  external_config.source_uris = "gs://dev_bucket_aischool/natality*"
  external_config.autodetect = True
  external_config.options.skip_leading_rows = 0
  # Configure the load job
  job_config = bigquery.LoadJobConfig(
      autodetect = True,
      skip_leading_rows = 1,
      ignore_unknown_values = False,
      max_bad_records = 0,
      # The source format defaults to CSV. The line below is optional.
      source_format=bigquery.SourceFormat.CSV
  )
  uri = 'gs://dev_bucket_aischool/natality*'
  destination_table_ref = dataset.table('natal')
  # Start the load job
  load_job = client.load_table_from_uri(
      uri, destination_table_ref, job_config=job_config)
  print('Starting job {}'.format(load_job.job_id))

  load_job.result()  # Waits for table load to complete.
  print('Job finished.')

  # Define the query
  QUERY_1 = """
      SELECT
              state AS Estado,
              SUM(CASE WHEN year >= 1970 AND year <1980 THEN plurality ELSE 0 END) AS B70,
              SUM(CASE WHEN year >= 1980 AND year <1990 THEN plurality ELSE 0 END) AS B80,
              SUM(CASE WHEN year >= 1990 AND year <2000 THEN plurality ELSE 0 END) AS B90,
              SUM(CASE WHEN year >= 2000 AND year <=2010 THEN plurality ELSE 0 END) AS B00
              
      FROM 
        `bigquery-public-data.samples.natality`
      WHERE 
        NOT IS_NAN(plurality) AND plurality > 1 AND state IS NOT NULL
      GROUP BY
        state
      ORDER BY
        state
  """

  # Define the parameter values in a query job configuration
  query_job = client.query(QUERY_1)
  # Return the results as a pandas DataFrame
  df = query_job.to_dataframe()
  df.to_csv("mrusso@paradigmadigital.csv")


if __name__ == '__main__':
    main()