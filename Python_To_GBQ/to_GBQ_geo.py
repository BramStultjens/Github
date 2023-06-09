from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery
import json

def main():
    # Load the Google Ads API client from the specified yaml file.
    yaml_file_path = r'\path\to\google-ads.yaml'
    client = GoogleAdsClient.load_from_storage(yaml_file_path)
    client_customer_id = "xxxxxxxx"  # Replace with your client customer ID.
    dataset_id = 'GBQ dataset ID'  # Replace with your BigQuery dataset ID.
    table_id = 'GBQ table ID'  # Replace with your BigQuery table ID.
    list_campaign_extension_settings(client, client_customer_id, dataset_id, table_id)

def list_campaign_extension_settings(client, client_customer_id, dataset_id, table_id):
    # Initialize the GoogleAdsService.
    google_ads_service = client.get_service("GoogleAdsService")

    # Construct a GAQL query to fetch the CampaignExtensionSetting data.
    query = '''
        SELECT
            geo_target_constant.resource_name,
            geo_target_constant.country_code

        FROM
            geo_target_constant
    '''

    # Execute the GAQL query.
    response = google_ads_service.search(customer_id=client_customer_id, query=query)

    for row in response:
        geo = row.geo_target_constant
        print(geo)

    # Create a BigQuery client.
    bigquery_client = bigquery.Client()

    # Get the BigQuery dataset and table.
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)

    # Define the schema of the BigQuery table.
    schema = [
        bigquery.SchemaField("resource_name", "STRING"),
        bigquery.SchemaField("country_code", "STRING"),
    ]

    # Prepare rows for insertion.
    rows = []
    for row in response:
        geo = row.geo_target_constant
        rows.append((
            geo.resource_name,
            geo.country_code
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
