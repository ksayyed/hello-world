from google.cloud import bigquery

def update_query_standard_sql(dataset_id, table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False

    query = ('UPDATE Parcel_Dataset.Parcels Original '
    'SET Original.Content_Value = Temp.Content_Value, '
    'Original.Fragile = Temp.Fragile, '
    'Original.Customer_Type = Temp.Customer_Type '
    'FROM Parcel_Dataset.Parcels_Temp as Temp WHERE Original.id = Temp.id')

    query_job = client.query(query, job_config=job_config)

    #statistics.query.numDmlAffectedRows or num_dml_affected_rows
    query_job.result()   # Waits for job to complete.
    
    print('Updated {} rows from {}:{}.'.format(
    query_job.num_dml_affected_rows, dataset_id, table_id))


def insert_query_standard_sql(dataset_id, table_id):
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig()

    # Set use_legacy_sql to False to use standard SQL syntax.
    # Note that queries are treated as standard SQL by default.
    job_config.use_legacy_sql = False

    query = (
    'INSERT Parcel_Dataset.Parcels ' 
    '(id, Tracking_Number, Sender_Name, Recipient_Name, Origination_PCode, Destination_PCode, Origination_City, Destination_City, '
    'Origination_State, Destination_State, Category, Order_Date, Expected_Delivery_Date, Actual_Delivery_Date, Status, ' 
    'Reason_for_Delay, Delay_in_Days, Delivery_Comments, Content_Value, Fragile, Customer_Type) '
    '(SELECT Temp.id, Temp.Tracking_Number, Temp.Sender_Name, Temp.Recipient_Name, Temp.Origination_PCode, Temp.Destination_PCode, Temp.Origination_City, '
    'Temp.Destination_City, Temp.Origination_State, Temp.Destination_State, Temp.Category, Temp.Order_Date, Temp.Expected_Delivery_Date, Temp.Actual_Delivery_Date, Temp.Status, '
    'Temp.Reason_for_Delay, Temp.Delay_in_Days, Temp.Delivery_Comments, Temp.Content_Value, Temp.Fragile, Temp.Customer_Type '
    'FROM Parcel_Dataset.Parcels_Temp as Temp '
    'WHERE Temp.Id not in (select id from Parcel_Dataset.Parcels))'
    )

    query_job = client.query(query, job_config=job_config)

    #statistics.query.numDmlAffectedRows or num_dml_affected_rows
    query_job.result()   # Waits for job to complete.
    
    print('Inserted {} rows from {}:{}.'.format(
    query_job.num_dml_affected_rows, dataset_id, table_id))


if __name__ == '__main__':
    
    dataset_id = 'Parcel_Dataset'
    #table_id = 'Parcel_Customer'
    table_id = 'Parcels'
    
    update_query_standard_sql(dataset_id, table_id)
    insert_query_standard_sql(dataset_id, table_id)