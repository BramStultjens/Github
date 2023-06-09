from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery

def main():
    # Load the Google Ads API client from the specified yaml file.
    yaml_file_path = r'\path\to\google-ads.yaml'
    client = GoogleAdsClient.load_from_storage(yaml_file_path)
    client_customer_id = "xxxxxxxx"  # Replace with your client customer ID.
    dataset_id = 'GBQ dataset ID'  # Replace with your BigQuery dataset ID.
    table_id = 'GBQ table ID'  # Replace with your BigQuery table ID.
    list_accounts(client, client_customer_id, dataset_id, table_id)

def list_accounts(client, client_customer_id, dataset_id, table_id):
    # Initialize the GoogleAdsService.
    google_ads_service = client.get_service("GoogleAdsService")

    # Construct a GAQL query to fetch the search term view data.
    query = '''
        SELECT  asset.image_asset.file_size, 
                asset.id, 
                asset.resource_name,
                asset.name,
                asset.text_asset.text,
                asset.call_asset.phone_number 
        FROM asset
    '''

    # Execute the GAQL query.
    response = google_ads_service.search(customer_id=client_customer_id, query=query)

    for row in response:
        asset = row.asset
        print(asset)

    # Create a BigQuery client.
    bigquery_client = bigquery.Client()

    # Get the BigQuery dataset and table.
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)

    # Define the schema of the BigQuery table.
    schema = [
        bigquery.SchemaField("image_asset-files_size", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("resource_name", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("text_asset-text", "STRING"),
        bigquery.SchemaField("call_asset-phone_number", "STRING")
    ]

    # Prepare rows for insertion.
    rows = []
    for row in response:
        asset = row.asset
        rows.append((
            asset.image_asset.file_size,
            asset.id,
            asset.resource_name,
            asset.name,
            asset.text_asset.text,
            asset.call_asset.phone_number
        ))

    # Insert rows into BigQuery.
    errors = bigquery_client.insert_rows(table, rows, selected_fields=schema)
    if errors:
        print('Errors occurred while inserting rows:')
        for error in errors:
            print(error)
    else:
        print('Data inserted successfully.')

if __name__ == '__main__':
    main()
