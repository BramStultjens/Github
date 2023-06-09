from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery

def main():
    # Load the Google Ads API client from the specified yaml file.
    yaml_file_path = r'\path\to\google-ads.yaml'
    client = GoogleAdsClient.load_from_storage(yaml_file_path)
    client_customer_id = "xxxxxxxx"  # Replace with your client customer ID.
    dataset_id = 'GBQ dataset ID'  # Replace with your BigQuery dataset ID.
    table_id = 'GBQ table ID'  # Replace with your BigQuery table ID.
    list_bidding_strategies(client, client_customer_id, dataset_id, table_id)

def list_bidding_strategies(client, client_customer_id, dataset_id, table_id):
    # Initialize the GoogleAdsService.
    google_ads_service = client.get_service("GoogleAdsService")

    # Construct a GAQL query to fetch the BiddingStrategy data.
    query = '''
        SELECT
            accessible_bidding_strategy.name, 
            accessible_bidding_strategy.resource_name, 
            accessible_bidding_strategy.type 
        FROM accessible_bidding_strategy
    '''

    # Execute the GAQL query.
    response = google_ads_service.search(customer_id=client_customer_id, query=query)

    for row in response:
        bidding_strategy = row.accessible_bidding_strategy
        print(bidding_strategy)

    # Create a BigQuery client.
    bigquery_client = bigquery.Client()

    # Get the BigQuery dataset and table.
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)

    # Define the schema of the BigQuery table.
    schema = [
        bigquery.SchemaField("resource_name", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
    ]

    # Prepare rows for insertion.
    rows = []
    for row in response:
        bidding_strategy = row.accessible_bidding_strategy
        rows.append((
            bidding_strategy.resource_name,
            bidding_strategy.name,
            bidding_strategy.type,
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
