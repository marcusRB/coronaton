# Importamos la librería bigquery
from google.cloud import bigquery
# Importamos la librería de cuenta de servicios de BQ
from google.oauth2 import service_account
import pandas as pd
import os

# Construct a BigQuery client object.
proj_ID = 'aischool-272715'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '../DS/Challenges/CORONATON/key/credentials.json'
#!export GOOGLE_APPLICATION_CREDENTIALS = '/home/jovyan/DS/Challenges/CORONATON/key/credentials.json'
#creds = service_account.Credentials.from_service_account_file('key/credentials.json')
#scoped_credentials = creds.with_scopes(
#    ['https://www.googleapis.com/auth/cloud-platform'])
#client = bigquery.Client(credentials=creds, project=proj_ID)
client = bigquery.Client()


## CREATE DATASET
datasets = list(client.list_datasets())  # Make an API request.
project = client.project
dataID = 'natal'
dataset_id = "{0}.{1}".format(client.project, dataID)

def createDataset(dataID, dataset_id):

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_id)
    #Specify the geographic location where the dataset should reside.
    dataset.location = "US"
    # Send the dataset to the API for creation.
    dataset = client.create_dataset(dataset)  # Make an API request.
    print("Created dataset {}.{}".format(client.project, dataset.dataset_id))
    
def deleteDataset(dataset_id):
    client.delete_dataset(dataset_id, 
                          delete_contents=True, not_found_ok=True)
    print("Deleted dataset '{}'.".format(dataset_id))
    


bq_dataset = []
for dataset in datasets:
    bq_dataset.append(dataset.dataset_id)

if dataID in bq_dataset:
    deleteDataset(dataset_id)
    createDataset(dataID,dataset_id)    
else:
    createDataset(dataID,dataset_id)

## CREATE TABLE
dataset_ref = client.dataset(dataID)
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
tableName = "natal"
dataset_ref_table = dataset_ref.table(tableName)
load_job = client.load_table_from_uri(uri, dataset_ref_table, job_config=job_config)  # API request
print("Starting job {}".format(load_job.job_id))

load_job.result()  # Waits for table load to complete.
print("Job finished.")


table = client.get_table(dataset_ref_table)  # Make an API request.


# Define the query1
QUERY_1 = """
    SELECT
            state AS Estado,
            SUM(CASE WHEN year >= 1970 AND year <1980 THEN plurality ELSE 0 END) AS B70,
            SUM(CASE WHEN year >= 1980 AND year <1990 THEN plurality ELSE 0 END) AS B80,
            SUM(CASE WHEN year >= 1990 AND year <2000 THEN plurality ELSE 0 END) AS B90,
            SUM(CASE WHEN year >= 2000 AND year <=2010 THEN plurality ELSE 0 END) AS B00
            
    FROM 
      `{project}.{dataset}.{table}`
    WHERE 
      NOT IS_NAN(plurality) AND plurality > 1 AND state IS NOT NULL
    GROUP BY
      state
    ORDER BY
      state
""".format(project=table.project, dataset=table.dataset_id, table=table.table_id)


# Define the parameter values in a query job configuration
query_job = client.query(QUERY_1)
# Return the results as a pandas DataFrame
df1 = query_job.to_dataframe()
print("Finish Query_1")

# Define the query2
QUERY_2 = """
WITH Race70 AS(
SELECT
    state as Estado70,
    count(*) as counter70,
  (CASE
  	WHEN child_race = 1 THEN "White"
 	WHEN child_race = 2 THEN "Black"
  	WHEN child_race = 3 THEN "American Indian"
  	WHEN child_race = 4 THEN "Chinese"
  	WHEN child_race = 5 THEN "Japanese"
  	WHEN child_race = 6 THEN "Hawaiian"
  	WHEN child_race = 7 THEN "Filipino"
	WHEN child_race = 18 THEN "Asian Indian"
	WHEN child_race = 28 THEN "Korean"
	WHEN child_race = 39 THEN "Samoan"
	WHEN child_race = 48 THEN "Vietnamese"
  	ELSE "Unknown/Other"
  END) as raza70 
  FROM (
    SELECT year, state, child_race
    FROM `{project}.{dataset}.{table}`)
    WHERE state IS NOT NULL AND child_race IS NOT NULL
    AND year >= 1970 AND year <1980
    GROUP BY state, raza70
    ),
Race80 AS(
	SELECT
		state as Estado80,
    	count(*) as counter80,
	  (CASE
	  	WHEN child_race = 1 THEN "White"
	 	WHEN child_race = 2 THEN "Black"
	  	WHEN child_race = 3 THEN "American Indian"
	  	WHEN child_race = 4 THEN "Chinese"
	  	WHEN child_race = 5 THEN "Japanese"
	  	WHEN child_race = 6 THEN "Hawaiian"
	  	WHEN child_race = 7 THEN "Filipino"
		WHEN child_race = 18 THEN "Asian Indian"
		WHEN child_race = 28 THEN "Korean"
		WHEN child_race = 39 THEN "Samoan"
		WHEN child_race = 48 THEN "Vietnamese"
	  	ELSE "Unknown/Other"
	  END) as raza80,
	  FROM (
	    SELECT year, state, child_race
	    FROM `{project}.{dataset}.{table}`)
	    WHERE state IS NOT NULL AND child_race IS NOT NULL
	    AND year >= 1980 AND year <1990
	    GROUP BY state, raza80
	    ),
Race90 AS(
	SELECT
		state as Estado90,
    	count(*) as counter90,
	  (CASE
	  	WHEN child_race = 1 THEN "White"
	 	WHEN child_race = 2 THEN "Black"
	  	WHEN child_race = 3 THEN "American Indian"
	  	WHEN child_race = 4 THEN "Chinese"
	  	WHEN child_race = 5 THEN "Japanese"
	  	WHEN child_race = 6 THEN "Hawaiian"
	  	WHEN child_race = 7 THEN "Filipino"
		WHEN child_race = 18 THEN "Asian Indian"
		WHEN child_race = 28 THEN "Korean"
		WHEN child_race = 39 THEN "Samoan"
		WHEN child_race = 48 THEN "Vietnamese"
	  	ELSE "Unknown/Other"
	  END) as raza90,
	  FROM (
	    SELECT year, state, child_race
	    FROM `{project}.{dataset}.{table}`)
	    WHERE state IS NOT NULL AND child_race IS NOT NULL
	    AND year >= 1990 AND year <2000
	    GROUP BY state, raza90
	    ),
Race00 AS(
	SELECT
		state as Estado00,
    	count(*) as counter00,
	  (CASE
	  	WHEN child_race = 1 THEN "White"
	 	WHEN child_race = 2 THEN "Black"
	  	WHEN child_race = 3 THEN "American Indian"
	  	WHEN child_race = 4 THEN "Chinese"
	  	WHEN child_race = 5 THEN "Japanese"
	  	WHEN child_race = 6 THEN "Hawaiian"
	  	WHEN child_race = 7 THEN "Filipino"
		WHEN child_race = 18 THEN "Asian Indian"
		WHEN child_race = 28 THEN "Korean"
		WHEN child_race = 39 THEN "Samoan"
		WHEN child_race = 48 THEN "Vietnamese"
	  	ELSE "Unknown/Other"
	  END) as raza00,
	  FROM (
	    SELECT year, state, child_race
	    FROM `{project}.{dataset}.{table}`)
	    WHERE state IS NOT NULL AND child_race IS NOT NULL
	    AND year >= 2000 AND year <=2010
	    GROUP BY state, raza00
	    )
SELECT
	r70.Estado70,
	r70.raza70 as Race70,
	r80.raza80 as Race80,
	r90.raza90 as Race90,
    r00.raza00 as Race00
FROM (
  SELECT
  ARRAY_AGG(Race70 ORDER BY Race70.counter70 DESC LIMIT 1)[OFFSET(0)] AS r70,
  ARRAY_AGG(Race80 ORDER BY Race80.counter80 DESC LIMIT 1)[OFFSET(0)] AS r80,
  ARRAY_AGG(Race90 ORDER BY Race90.counter90 DESC LIMIT 1)[OFFSET(0)] AS r90,
  ARRAY_AGG(Race00 ORDER BY Race00.counter00 DESC LIMIT 1)[OFFSET(0)] AS r00
  FROM Race70, Race80, Race90, Race00
  WHERE Race70.Estado70 = Race80.Estado80
  GROUP BY Race70.Estado70
  ORDER BY Race70.Estado70 ASC
 )
""".format(project=table.project, dataset=table.dataset_id, table=table.table_id)



# Define the parameter values in a query job configuration
query_job = client.query(QUERY_2)
# Return the results as a pandas DataFrame
df2 = query_job.to_dataframe()
print("Finish Query_2")


# Define the query3
QUERY_3 = """
    SELECT
            state AS Estado,
            SUM(IF(is_male = TRUE, 1, 0)) AS Male,
            SUM(IF(is_male = FALSE, 1, 0)) AS Female,
            ROUND(AVG((weight_pounds * 0.453592)),3) AS Weight
    FROM 
      `{project}.{dataset}.{table}`
    WHERE 
      NOT IS_NAN(plurality) AND plurality > 1 AND state IS NOT NULL
    GROUP BY
      state
    ORDER BY
      state
""".format(project=table.project, dataset=table.dataset_id, table=table.table_id)


# Define the parameter values in a query job configuration
query_job = client.query(QUERY_3)
# Return the results as a pandas DataFrame
df3 = query_job.to_dataframe()
print("Finish Query_3")

# Create dataframe
data = df1.join(df2.iloc[:,1:])
data = data.join(df3.iloc[:,1:])

# Write destination
destination = f'gs://aischool_dataoutput/mrusso@paradigmadigital.csv'
data.to_csv(destination, index=None, encoding="UTF-8", sep=",")