from google.cloud import bigquery

def load_data_from_gcs(dataset_id, table_id, source):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = False
    job_config.source_format = 'CSV'
    job_config.skip_leading_rows = 1
    job_config.write_disposition = 'WRITE_TRUNCATE'  # WRITE_EMPTY, WRITE_APPEND, WRITE_TRUNCATE

    job = bigquery_client.load_table_from_uri(source, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))


if __name__ == '__main__':
    dataset_id = 'Parcel_Dataset'
    #table_id = 'Parcel_Customer_Temp'
    table_id = 'Parcels_Temp'
    
    #source = 'gs://ap-parcel-data-dev/PARCEL_DATA_19032018.csv'
    source = 'gs://ap-parcel-data-test/PARCEL_DATA_19032018.csv'
    #source = 'gs://ap-parcel-data-prod/PARCEL_DATA_19032018.csv'
    
    load_data_from_gcs(
        dataset_id,
        table_id,
        source)