from google.cloud import bigquery

def add_empty_column(dataset_id, table_id):
    #Adds empty columns to an existing table.

    bigquery_client = bigquery.Client()
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)
    table = bigquery_client.get_table(table_ref)  # API request
    
    original_schema = table.schema
    new_schema = original_schema[:]  # creates a copy of the schema

    new_schema.append(bigquery.SchemaField('Content_Value', 'FLOAT', mode='NULLABLE'))
    new_schema.append(bigquery.SchemaField('Fragile', 'BOOLEAN',mode='NULLABLE'))
    new_schema.append(bigquery.SchemaField('Customer_Type', 'STRING', mode='NULLABLE'))

    table.schema = new_schema
    table = bigquery_client.update_table(table, ['schema'])  # API request
    
    columns_added =  len(new_schema) - len(original_schema)
    
    print('Added {} empty columns to {}:{}.'.format(columns_added, dataset_id, table_id))
    
if __name__ == '__main__':
    dataset_id = 'Parcel_Dataset'
    
    table_id = 'Parcels'
    add_empty_column(dataset_id, table_id)
    
    table_id = 'Parcels_Temp'
    add_empty_column(dataset_id, table_id)
    