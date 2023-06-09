from google.ads.googleads.client import GoogleAdsClient
from google.cloud import bigquery

def main():
    # Load the Google Ads API client from the specified yaml file.
    yaml_file_path = r'\path\to\google-ads.yaml'
    client = GoogleAdsClient.load_from_storage(yaml_file_path)
    client_customer_id = "xxxxxxxx"  # Replace with your client customer ID.
    dataset_id = 'GBQ dataset ID'  # Replace with your BigQuery dataset ID.
    table_id = 'GBQ table ID'  # Replace with your BigQuery table ID.
    list_campaign_conversion_goals(client, client_customer_id, dataset_id, table_id)

def list_campaign_conversion_goals(client, client_customer_id, dataset_id, table_id):
    # Initialize the GoogleAdsService.
    google_ads_service = client.get_service("GoogleAdsService")

    # Construct a GAQL query to fetch the necessary data.
    query = f'''
        SELECT
            campaign_conversion_goal.biddable,
            campaign_conversion_goal.campaign,
            campaign_conversion_goal.category,
            campaign_conversion_goal.resource_name,
            campaign_conversion_goal.origin
        FROM
            campaign_conversion_goal
    '''

    # Execute the GAQL query.
    response = google_ads_service.search(customer_id=client_customer_id, query=query)

    for row in response:
        campaign = row.campaign_conversion_goal
        print(f'Campaign ID: {campaign.biddable}')
        print(f'Campaign Name: {campaign.campaign}')
        print(f'Conversion Action: {campaign.category}')
        print(f'Resource Name: {campaign.resource_name}')
        print(f'Resource Name: {campaign.origin}')

    # Create a BigQuery client.
    bigquery_client = bigquery.Client()

    # Get the BigQuery dataset and table.
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = bigquery_client.get_table(table_ref)

    # Define the schema of the BigQuery table.
    schema = [
        bigquery.SchemaField("biddable", "BOOL"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("resource_name", "STRING"),
        bigquery.SchemaField("origin", "STRING")
    ]

    # Prepare rows for insertion.
    rows = []
    for row in response:
        campaign = row.campaign_conversion_goal
        rows.append((
            campaign.biddable,
            campaign.campaign,
            campaign.category,
            campaign.resource_name,
            campaign.origin
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
