from google.cloud import bigquery

def query_standard_sql():
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False

    query = 'select * from Parcel_Dataset.Parcels where id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10) OR Id > 10000'

    query_job = client.query(query, job_config=job_config)

    # Print the results.
    for row in query_job.result():  # Waits for job to complete.
        print(row)
    
if __name__ == '__main__':
    query_standard_sql()