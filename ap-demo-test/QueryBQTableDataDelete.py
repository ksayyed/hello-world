from google.cloud import bigquery

def delete_query_standard_sql(query, dataset_id, table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False

    query_job = client.query(query, job_config=job_config)

    #statistics.query.numDmlAffectedRows or num_dml_affected_rows
    query_job.result()   # Waits for job to complete.
    
    print('Deleted {} rows from {}:{}.'.format(
    query_job.num_dml_affected_rows, dataset_id, table_id))
    
if __name__ == '__main__':
    query = 'Delete from Parcel_Dataset.Parcels where id is not Null'
    dataset_id = 'Parcel_Dataset'
    table_id = 'Parcels'
    delete_query_standard_sql(query, dataset_id, table_id)
    
    query = 'Delete from Parcel_Dataset.Parcels_Temp where id is not Null'
    dataset_id = 'Parcel_Dataset'
    table_id = 'Parcels_Temp'
    delete_query_standard_sql(query, dataset_id, table_id)