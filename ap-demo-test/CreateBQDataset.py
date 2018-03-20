from google.cloud import bigquery

def create_dataset():
    client = bigquery.Client()
    dataset_id = 'Parcel_Dataset'

    # Create a DatasetReference using a chosen dataset ID.
    # The project defaults to the Client's project if not specified.
    dataset_ref = client.dataset(dataset_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(dataset_ref)
    # Specify the geographic location where the dataset should reside.
    dataset.location = 'US'

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.AlreadyExists if the Dataset already
    # exists within the project.
    dataset = client.create_dataset(dataset)  # API request

    print('Created dataset {}.'.format(
    dataset_id))


if __name__ == '__main__':
    create_dataset()