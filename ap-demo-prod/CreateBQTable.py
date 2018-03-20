from google.cloud import bigquery

def create_table_with_schema(dataset_id, table_id):
    #Create new table within existing dataset.
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)

    schema = [
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('Tracking_Number', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Sender_Name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Recipient_Name', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Origination_PCode', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Destination_PCode', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Origination_City', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Destination_City', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Origination_State', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Destination_State', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Category', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Order_Date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('Expected_Delivery_Date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('Actual_Delivery_Date', 'DATE', mode='NULLABLE'),
        bigquery.SchemaField('Status', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Reason_for_Delay', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('Delay_in_Days', 'INTEGER', mode='NULLABLE'),
        bigquery.SchemaField('Delivery_Comments', 'STRING', mode='NULLABLE'),
    ]

    table_ref = dataset_ref.table(table_id)
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)  # API request

    assert table.table_id == table_id

    print('Created table {} for dataset {}.'.format(
    table_id, dataset_id))

if __name__ == '__main__':
    dataset_id = 'Parcel_Dataset'

    table_id = 'Parcels'
    create_table_with_schema(dataset_id, table_id)
    
    table_id = 'Parcels_Temp'
    create_table_with_schema(dataset_id, table_id)