from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery

def main():
    # Load the Google Ads API client from the specified yaml file.
    yaml_file_path = r'\path\to\google-ads.yaml'
    client = GoogleAdsClient.load_from_storage(yaml_file_path)
    client_customer_id = "xxxxxxxx"  # Replace with your client customer ID.
    dataset_id = 'GBQ dataset ID'  # Replace with your BigQuery dataset ID.
    table_id = 'GBQ table ID'  # Replace with your BigQuery table ID.
    list_conversion_actions(client, client_customer_id, dataset_id, table_id)

def list_conversion_actions(client, client_customer_id, dataset_id, table_id):
    # Initialize the GoogleAdsService.
    google_ads_service = client.get_service("GoogleAdsService")

    # Construct a GAQL query to fetch the BiddingStrategy data.
    query = '''
        SELECT  conversion_action.resource_name,
                conversion_action.type, 
                conversion_action.category 
        FROM    conversion_action
    '''

    # Execute the GAQL query.
    response = google_ads_service.search(customer_id=client_customer_id, query=query)

    for row in response:
        conversion = row.conversion_action
        print(conversion)

    # Create a BigQuery client.
    bigquery_client = bigquery.Client()

    # Get the BigQuery dataset and table.
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)

    # Define the schema of the BigQuery table.
    schema = [
        bigquery.SchemaField("resource_name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("category", "STRING"),
    ]

    # Prepare rows for insertion.
    rows = []
    for row in response:
        conversion = row.conversion_action
        rows.append((
            conversion.resource_name,
            conversion.type,
            conversion.category
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
